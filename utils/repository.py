import sqlite3
import threading
from abc import abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from dateutil import parser
from openai import OpenAI
from pymilvus import (
    FieldSchema, DataType, MilvusClient, CollectionSchema, Collection, )

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
    def books(self) -> list[Book]: ...

    @abstractmethod  # type: ignore[misc]
    def reviews(self) -> list[Review]: ...

    @abstractmethod  # type: ignore[misc]
    def save(self, book: Book, source: str = "") -> None: ...

    @abstractmethod  # type: ignore[misc]
    def save_book(self, book: Book) -> None: ...

    @abstractmethod  # type: ignore[misc]
    def save_reviews(self, reviews: list[Review]) -> None: ...

    @abstractmethod  # type: ignore[misc]
    def destroy(self) -> None: ...


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

    def __init__(self, db_path: str):
        self._db_path = Path(db_path)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._conn:
            # executescript allows running multiple statements at once
            self._conn.executescript(self._DDL)

    # ----------------------- context‑manager -------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    # ---------------------------- API -------------------------------

    def exists(self, book_id: int) -> bool:
        with self._lock, self._conn as c:
            row = c.execute("SELECT 1 FROM books WHERE id = ?",
                            (book_id,)).fetchone()
            return row is not None

    def books(self) -> list[Book]:
        with self._lock, self._conn as c:
            rows = c.execute("SELECT * FROM books").fetchall()
            return [Book(**row) for row in rows]

    def reviews(self) -> list[Review]:
        with self._lock, self._conn as c:
            rows = c.execute("SELECT * FROM book_reviews").fetchall()
            return [Review(**row) for row in rows]

    def save(self, book: Book, source: str = "bookmeter"):
        self.save_book(book)
        self.save_reviews(
            [Review(book_id=book.id, review=r, source=source) for r in book.reviews])

    def save_book(self, book: Book) -> None:
        """Add a Book without reviews"""
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
        """Write reviews in batch, de-duped by UNIQUE(book_id, source, review)"""
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

    def destroy(self) -> None:
        self._db_path.unlink()


class MilvusRepository:
    """Thread‑safe Milvus implementation (books + book_reviews)."""

    # —— collections —— #
    BOOKS_COL = "books"
    REV_COL = "book_reviews"

    # —— dimension —— #
    TITLE_VEC_DIM = 512
    REVIEW_VEC_DIM = 1536

    # —— schema —— #
    _books_fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="title_vec", dtype=DataType.FLOAT_VECTOR, max_length=TITLE_VEC_DIM),
        FieldSchema(name="author", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="url", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="published_at_ts", dtype=DataType.INT64),  # epoch 秒
        FieldSchema(name="image_url", dtype=DataType.VARCHAR, max_length=1024),
        FieldSchema(name="page", dtype=DataType.INT64),
        FieldSchema(name="registration_count", dtype=DataType.INT64),
    ]

    _reviews_fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="review_vec", dtype=DataType.FLOAT_VECTOR, dim=REVIEW_VEC_DIM),
        FieldSchema(name="book_id", dtype=DataType.INT64),
        FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=64),
        FieldSchema(name="review", dtype=DataType.VARCHAR, max_length=8192),
    ]

    def __init__(
            self,
            milvus_uri: str,
            milvus_token: str,
            openai_api_key: str,
    ):
        self._milvus_client = MilvusClient(uri=milvus_uri, token=milvus_token)
        self._openai_client = OpenAI(api_key=openai_api_key)
        self._lock = threading.Lock()

        self._books: Collection = self._ensure_books_collections()
        self._reviews: Collection = self._ensure_reviews_collection()

    # ------------------- context‑manager API ----------------------- #
    def __enter__(self):  # noqa
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa
        self._milvus_client.close()

    def _ensure_books_collections(self) -> Collection:
        """Create the book collection"""

        if not self._milvus_client.has_collection(self.BOOKS_COL):
            logger.info("Books collection does not exist, creating...")
            book_schema = CollectionSchema(fields=self._books_fields, description='Books')
            books_collection: Collection = Collection(name=self.BOOKS_COL, schema=book_schema)
            books_collection.create_index("title_vec", {
                "index_type": "HNSW",
                "metric_type": "IP",
                "params": {"M": 32, "efConstruction": 200}
            })
        return self._milvus_client.describe_collection(self.BOOKS_COL)

    def _ensure_reviews_collection(self) -> Collection:
        """Create the review collection"""

        if not self._milvus_client.has_collection(self.REV_COL):
            logger.info("Reviews collection does not exist, creating...")
            review_schema = CollectionSchema(fields=self._reviews_fields, description='Bookreviews')
            reviews_collection: Collection = Collection(name=self.REV_COL, schema=review_schema)
            reviews_collection.create_index("review_vec", {
                "index_type": "IVF_FLAT",
                "metric_type": "IP",
                "params": {"nlist": 2048}
            })
        return self._milvus_client.describe_collection(self.REV_COL)

    def _embed_text(self, text: str, *, dim: int = 1536) -> list[float]:
        """Embed text using OpenAI's API"""
        resp = self._openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
            dimensions=dim  # clip dimensions
        )
        return resp.data[0].embedding

    # -------------------- BookRepository 接口 ---------------------- #
    def exists(self, book_id: int) -> bool:
        expr = f"id == {book_id}"
        res = self._books.query(expr, output_fields=["id"], limit=1)
        return len(res) > 0

    def books(self) -> list[Book]:
        expr = "id != 0"
        res = self._books.query(expr,
                                output_fields=[f.name for f in self._books_fields if
                                               not str(f.dtype).upper().endswith("VECTOR")],
                                limit=0)
        return [Book(**row) for row in res]

    def reviews(self) -> list[Review]:
        expr = "id != 0"
        res = self._reviews.query(expr,
                                  output_fields=[f.name for f in self._reviews_fields if
                                                 not str(f.dtype).upper().endswith("VECTOR")],
                                  limit=0)
        return [Review(**row) for row in res]

    def save(self, book: Book, source: str = "bookmeter"):
        self.save_book(book)
        self.save_reviews(
            [Review(book_id=book.id, review=r, source=source) for r in book.reviews]
        )

    def save_book(self, book: Book) -> None:
        with self._lock:
            title_vec = self._embed_text(book.title, dim=self.TITLE_VEC_DIM)
            self._books.upsert([
                [book.id],
                title_vec,
                [book.title],
                [book.author],
                [book.url],
                [self._epoch(book.published_at)],
                [book.image_url],
                [book.page],
                [book.registration_count],
            ])

    def save_reviews(self, reviews: list[Review]) -> None:
        if not reviews:
            return

        with self._lock:
            rows = [
                {"book_id": r.book_id,
                 "source": r.source,
                 "review": r.review,
                 "review_vec": self._embed_text(r.review, dim=self.REVIEW_VEC_DIM)}
                for r in reviews
            ]
            self._milvus_client.insert(collection_name=self._reviews.name, data=rows)

    def destroy(self) -> None:
        self._milvus_client.drop_collection(self.BOOKS_COL)
        self._milvus_client.drop_collection(self.REV_COL)

    def sqlite2milvus(self, db_path: str) -> None:
        with SQLiteRepository(db_path) as repo:
            for book in repo.books():
                self.save_book(book)
            for review in repo.reviews():
                self.save_reviews([review])

    # ------------------------ helper ------------------------------ #
    @staticmethod
    def _epoch(dt: datetime | str) -> int:
        if isinstance(dt, datetime):
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        elif isinstance(dt, str):
            return int(parser.parse(dt).replace(tzinfo=timezone.utc).timestamp())
        else:
            raise ValueError(f"Invalid datetime type: {type(dt)}")
