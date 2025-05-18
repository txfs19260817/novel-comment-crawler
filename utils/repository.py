import sqlite3
import threading
from abc import abstractmethod
from typing import Optional, Protocol

from utils.logger import get_logger
from utils.types import Book, Review

logger = get_logger(__name__)


class BookRepository(Protocol):
    """Minimum interface a storage backend must implement."""

    # context‑manager API
    def __enter__(self): ...

    def __exit__(self, exc_type, exc_val, exc_tb): ...

    @abstractmethod  # type: ignore[misc]
    def exists(self, book_id: int) -> bool: ...

    @abstractmethod  # type: ignore[misc]
    def save(self, book: Book, source: str = "") -> None: ...

    @abstractmethod  # type: ignore[misc]
    def save_book(self, book: Book) -> None: ...

    @abstractmethod  # type: ignore[misc]
    def save_reviews(self, reviews: list[Review]) -> None: ...


class SQLiteRepository:
    """Thread‑safe SQLite implementation (books + book_reviews)."""

    _DDL = """
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY,
        title TEXT,
        author TEXT,
        url TEXT,
        published_at TIMESTAMP,
        image_url TEXT,
        page INTEGER,
        registration_count INTEGER
    );
    CREATE TABLE IF NOT EXISTS book_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        source TEXT NOT NULL,
        review TEXT NOT NULL,
        UNIQUE(book_id, source, review),
        FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_reviews_book_id ON book_reviews(book_id);
    """

    # -----------------------------------------------------------------

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._conn:
            # executescript 允许一次性跑多条语句
            self._conn.executescript(self._DDL)

    # ----------------------- context‑manager -------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    # ---------------------------- API -------------------------------

    # 保持原接口，供旧代码 _repo.exists(book_id) 直接使用
    def exists(self, book_id: int) -> bool:
        with self._lock, self._conn as c:
            row = c.execute("SELECT 1 FROM books WHERE id = ?", (book_id,)).fetchone()
            return row is not None

    def save(self, book: Book, source: str = "bookmeter"):
        self.save_book(book)
        self.save_reviews([Review(book_id=book.id, review=r, source=source) for r in book.reviews])

    # -------- 新接口：拆分存储 --------------------------------------

    def save_book(self, book: Book) -> None:
        """插入 / 更新一本书（无 reviews）。"""
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO books (id, title, author, url, published_at, image_url, page, registration_count)
                VALUES (:id, :title, :author, :url, :published_at, :image_url, :page,
                        :registration_count) ON CONFLICT(id) DO
                UPDATE SET
                    title = excluded.title,
                    author = excluded.author,
                    url = excluded.url,
                    published_at = excluded.published_at,
                    image_url = excluded.image_url,
                    page = excluded.page,
                    registration_count = excluded.registration_count
                """,
                book.__dict__,
            )

    def save_reviews(self, reviews: list[Review]) -> None:
        """
        批量写入评论。每条评论去重（UNIQUE(book_id, source, review)）。
        """
        if len(reviews) == 0:
            return
        with self._lock, self._conn:
            self._conn.executemany(
                """
                INSERT
                OR IGNORE INTO book_reviews (book_id, source, review)
                VALUES (:book_id, :source, :review)
                """,
                (r.__dict__ for r in reviews),
            )

    # --------- 便捷读取 ---------------------------------------------

    def get_reviews(self, book_id: int, *, source: Optional[str] = None) -> list[str]:
        sql = "SELECT review FROM book_reviews WHERE book_id = ?"
        params = [book_id]
        if source:
            sql += " AND source = ?"
            params.append(source)

        with self._lock, self._conn as c:
            rows = c.execute(sql, params).fetchall()
            return [row["review"] for row in rows]
