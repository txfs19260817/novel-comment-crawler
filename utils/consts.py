from urllib.parse import quote

URL = 'https://bookmeter.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",  # Do Not Track
    "Referer": "https://www.google.com",
    "Cache-Control": "max-age=0",
}
PLAYWRIGHT_ARGS = [
    "--js-flags=--max_old_space_size=512",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-sandbox",
    "--disable-software-rasterizer",
    "--mute-audio",
    "--blink-settings=imagesEnabled=false",  # Don't load images
]

def author_url(book_id: str | int, limit: int = 8):
    return f'{URL}/api/v1/books/{book_id}/related_books/author?limit={limit}'


def review_url(book_id: str | int, offset: int = 0, limit: int = 100):
    return f'{URL}/books/{book_id}/reviews.json?offset={offset}&limit={limit}'


def search_url(keyword: str, page: int = 1, partial: bool = True):
    encoded_keyword = quote(keyword)
    return f'{URL}/search?author=&keyword={encoded_keyword}&sort=release_date&type=japanese_v2&page={page}' + (
        '&partial=true' if partial else '')

def external_stores_url(book_id: str | int):
    return f'{URL}/api/v1/books/{book_id}/external_book_stores.json?'