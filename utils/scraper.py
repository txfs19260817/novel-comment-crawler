import asyncio
import json
import re
from datetime import timezone
from pathlib import Path
from typing import Optional, Any

from bs4 import BeautifulSoup, ResultSet, Tag
from dateutil import parser
from playwright.async_api import async_playwright, Page, BrowserContext
from tqdm.asyncio import tqdm as tqdm_async

from utils.consts import search_url, URL, author_url, review_url, external_stores_url
from utils.helpers import keep_first_last_curly_brackets, RetryQueue, RetryItem
from utils.httpclient import HttpClientAsync
from utils.logger import get_logger
from utils.repository import BookRepository, SQLiteRepository
from utils.types import Book, AuthorResponse, AuthorResource, ReviewListResponse, ReviewResource, ExternalStores, Review

logger = get_logger(__name__)


class BookmeterScraper:
    """Scrapes Bookmeter search → author → review pipeline (async version)."""

    def __init__(
            self,
            settings: Any,
            *,
            repo: Optional[BookRepository] = None,
    ):
        self._running = False
        self._settings = settings
        self._repo = repo or SQLiteRepository(f"{self._settings.save_filename}.db")
        self._http = HttpClientAsync()
        self._retry_queue = RetryQueue(
            max_size=self._settings.retry.retry_queue_size,
            max_retry_count=self._settings.retry.max_retry_count,
            backoff_factor=self._settings.retry.backoff_factor,
        )

    # --------------------------- Public API --------------------------- #

    async def run(self) -> None:
        """Run the scraper asynchronously."""

        logger.info("Starting scrape for %d keyword(s)", len(self._settings.search_keywords))
        with self._repo:  # repo is synchronized context
            async with async_playwright() as p:
                context: BrowserContext = await p.chromium.launch_persistent_context(
                    user_data_dir=Path(self._settings.browser_user_data),
                    headless=False,
                )

                # —— 1) 先登入一次，cookie 会在整个 context 里共享 ——
                first_tab: Page = await context.new_page()
                await self._login(first_tab)
                await first_tab.close()

                # —— 2) 创建并发任务，每关键字一个 tab ——
                semaphore = asyncio.Semaphore(self._settings.max_workers)
                tasks = [
                    asyncio.create_task(
                        self._keyword_worker(keyword, context, semaphore)
                    )
                    for keyword in self._settings.search_keywords
                ]

                # —— 3) 等待所有任务完成 ——
                self._running = True
                retry_task = asyncio.create_task(self._retry_worker(context))
                await asyncio.gather(*tasks)
                self._running = False
                await retry_task
                await context.close()
        logger.info("Scraping finished!")

        # --------------------------- Internals --------------------------- #

    async def _login(self, page: Page) -> None:
        logger.info("Logging in...")
        await page.goto(URL + "/login")
        try:
            await page.wait_for_url(URL + "/home", timeout=1 * 1000)
        except Exception:
            logger.warning(f"Failed to login to {URL}, trying to login again...")
            await page.fill("input[id=session_email_address]", self._settings.email)
            await page.fill("input[id=session_password]", self._settings.password)
            await page.click("#js_sessions_new_form > form > div.actions.common-margin-top5 > button")
        finally:
            await page.wait_for_url(URL + "/home", timeout=2 * 1000)
            logger.info("Logged in!")

    async def _keyword_worker(self, keyword: str, context, semaphore: asyncio.Semaphore) -> None:
        """为单个关键字开新 tab，抓完即关。"""
        async with semaphore:  # 控制并发
            logger.info(f"Keyword {keyword} started...")
            page = await context.new_page()
            try:
                await self._process_keyword(keyword, page)
            finally:
                await page.close()

    async def _process_keyword(self, keyword: str, page: Page) -> None:
        """Walk search result pages → authors → books."""
        async for page_no in tqdm_async(
                range(1, self._settings.max_search_pages + 1),
                desc=f"'{keyword}'",
                total=self._settings.max_search_pages
        ):
            book_ids = await self._search_ids(keyword, page_no, page)
            for book_id in book_ids:
                author_resp = await self._author(book_id, page)
                if not author_resp:
                    continue
                for res in author_resp.resources:
                    book: Optional[Book] = await self._build_book(res, page)
                    if self._wanted_book(book):
                        self._repo.save(book)
                        if self._settings.amazon.enable:
                            amazon_reviews: list[Review] = await self._amazon_reviews(book_id, page)
                            self._repo.save_reviews(amazon_reviews)

    # ------------------------ Scraping helpers ------------------------ #

    async def _search_ids(self, keyword: str, page_no: int, page: Page) -> set[int]:
        url = search_url(keyword, page_no, True)
        html = await self._fetch_with_playwright(url, page, empty_on_error=True)
        anchors: ResultSet[Tag] = BeautifulSoup(html, "html.parser").find_all("a")
        return {
            int(href.split("/")[-1])
            for a in anchors
            if (href := a.get("href", "")).startswith("/books/") and href.split("/")[-1].isdigit()
        }

    async def _author(self, book_id: int, page: Page) -> Optional[AuthorResponse]:
        html_raw = ""
        try:
            html_raw = await self._fetch_with_playwright(author_url(book_id), page, empty_on_error=False)
            json_dict = self._json_from_html(html_raw)
            return AuthorResponse.from_dict(json_dict)
        except Exception:
            logger.exception("Failed to fetch author info for: " + html_raw)
            return None

    async def _build_book(self, author_resource: AuthorResource, page: Page) -> Optional[Book]:
        html_raw = ""
        try:
            html_raw = await self._fetch_with_playwright(review_url(book_id=author_resource.id), page,
                                                         empty_on_error=False)
            reviews_json = self._json_from_html(html_raw)
            reviews: list[str] = [
                r.content
                for r in ReviewListResponse.from_dict(reviews_json).resources
                if self._wanted_review(r)
            ]
        except Exception:
            logger.exception(f"Failed to fetch review info for: {html_raw} , enqueuing retry queue")
            self._retry_queue.enqueue(RetryItem(author_resource.id, 0))
            return None
        return Book(
            id=author_resource.id,
            title=author_resource.title,
            author=author_resource.author.name,
            url=URL + author_resource.path,
            published_at=parser.parse(author_resource.published_at or "1970-01-01T00:00:00.000+09:00").astimezone(
                timezone.utc),
            image_url=author_resource.image_url,
            page=author_resource.page,
            registration_count=author_resource.registration_count,
            reviews=reviews,
        )

    async def _amazon_reviews(self, book_id: int, page: Page) -> list[Review]:
        """
        抓取 Amazon.co.jp 的评论：
        1) 用 Bookmeter API 找到商品页 URL
        2) 在商品页查找 data-hook="see-all-reviews-link-foot" 的链接
        3) 跳转到完整评论页后，循环翻页抓取每条评论的 标题/星级/正文
        4) 返回 ["<标题> <星数> <正文>", ...]
        """
        try:
            # 1) 取外部店铺列表
            json_dict = await self._http.get_json(external_stores_url(book_id))
            stores: ExternalStores = ExternalStores.from_dict(json_dict)
            if not stores or not stores.resources:
                return []
            amazon_url = next((r.url for r in stores.resources if r.alphabet_name.lower() == "amazon"), None)
            if not amazon_url:
                return []

            # 2) 打开商品页，找“レビューをすべて見る”链接
            await page.goto(amazon_url, wait_until="domcontentloaded")
            see_all = await page.query_selector('a[data-hook="see-all-reviews-link-foot"]')
            if not see_all:
                return []

            href = await see_all.get_attribute("href")
            if not "product-reviews" in href:
                return []
            # 有时候 href 是相对路径
            reviews_url = href if href.startswith("http") else f"https://www.amazon.co.jp{href}"

            reviews: list[Review] = []
            # 3) 循环翻页抓
            for pno in range(1, self._settings.amazon.max_review_pages + 1):
                # 如果 reviews_url 自带 pageNumber 参数，也可以直接替换或拼接
                url = reviews_url
                if "pageNumber=" in url:
                    url = re.sub(r"pageNumber=\d+", f"pageNumber={pno}", url)
                else:
                    url = f"{url}{"&" if "?" in url else "?"}pageNumber={pno}"

                html = await self._get_html(page, url)
                soup = BeautifulSoup(html, "html.parser")
                blocks: list[Tag] = soup.select('li[data-hook="review"]') or []

                for b in blocks:
                    title = b.select_one('a[data-hook="review-title"]')
                    body = b.select_one('span[data-hook="review-body"]')

                    parts = []
                    if title and title.text.strip():
                        parts.append(title.text.strip())
                    if body and body.text.strip():
                        parts.append(body.text.strip())

                    line = " ".join(parts)
                    if len(line) > 10:
                        reviews.append(Review(book_id, line, "amazon"))

                if len(blocks) < 10:
                    break

            return reviews

        except Exception:
            logger.exception("Failed to fetch Amazon reviews for %d", book_id)
            return []

    # ------------------------ Retry machinery ------------------------ #

    async def _retry_worker(self, context: BrowserContext) -> None:
        """后台重试失败 book_id，直到 _running=False 且队列清空。"""
        while self._running:
            if self._retry_queue.is_empty():
                logger.info("Retry worker sleeps because retry queue is empty")
                await asyncio.sleep(5)
                continue

            retry_item: RetryItem = self._retry_queue.dequeue()
            book_id, attempt = retry_item.id, retry_item.attempts
            logger.info("Retrying id=%s (attempt %s)...", book_id, attempt)
            if attempt > 1:
                await asyncio.sleep(self._retry_queue.backoff(attempt))
            try:
                page = await context.new_page()
                if author_resp := await self._author(book_id, page):
                    for res in author_resp.resources:
                        book = await self._build_book(res, page)
                        if self._wanted_book(book):
                            self._repo.save(book)
            except Exception:
                if self._retry_queue.can_retry(attempt):
                    # 失败后再次入队，attempt+1
                    self._retry_queue.enqueue(RetryItem(book_id, attempt + 1))
                    logger.warning("Retry %s failed for id=%s, will retry again (attempt %s)",
                                   attempt, book_id, attempt + 1)
                else:
                    logger.error("Giving up id=%s after %s attempts", book_id, attempt)
        logger.info(f"Retry worker finished, still has {len(self._retry_queue)} items in queue")

    # ----------------------------- Utils ----------------------------- #

    async def _fetch_with_playwright(self, url: str, page: Page, *, empty_on_error: bool = True) -> str:
        try:
            return await self._get_html(page, url)
        except Exception as e:
            logger.warning("Playwright failed for %s (%s) – falling back to httpx", url, e)
            try:
                return await self._http.get_text(url)
            except Exception as httpx_e:
                if empty_on_error:
                    logger.warning("Both Playwright and HttpClient failed for %s, returning empty", url)
                    return ""
                raise RuntimeError(f"Both Playwright and HttpClient failed for {url}") from httpx_e

    @staticmethod
    def _wanted_book(book: Optional[Book]) -> bool:
        return bool(book and book.reviews and "コミック" not in book.title)

    @staticmethod
    def _wanted_review(review: Optional[ReviewResource]) -> bool:
        return bool(review and review.content and len(review.content) > 10)

    @staticmethod
    async def _get_html(page: Page, url: str) -> str:
        response = await page.goto(url, wait_until="networkidle", timeout=60 * 1000)
        if response and response.status >= 400:
            raise RuntimeError(f"Bad status {response.status} for {url}")
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        return await page.content()

    @staticmethod
    def _json_from_html(html: str) -> dict:
        try:
            return json.loads(keep_first_last_curly_brackets(html))
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
            pre_element = soup.find("pre")
            if not pre_element or not pre_element.text:
                logger.warning("Unable to find <pre> element in HTML snippet: " + html)
                return {}
            try:
                return json.loads(keep_first_last_curly_brackets(pre_element.text))
            except Exception as e:
                logger.warning("Unable to extract JSON payload from HTML snippet: %s", e)
                return {}
