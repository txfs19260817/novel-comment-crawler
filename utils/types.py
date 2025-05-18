from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from dataclasses_json import dataclass_json


# ---------- Common ----------

@dataclass_json
@dataclass
class Metadata:
    sort: str = "score"
    order: str = "desc"
    offset: int = 0
    previous_cursor: int = 0
    next_cursor: int = 0
    limit: int = 0
    count: int = 0
    unread_count: Optional[int] = 0


@dataclass_json
@dataclass
class User:
    id: int = 0
    path: str = ""
    name: str = ""
    image: str = ""


# ---------- Review ----------

@dataclass_json
@dataclass
class Nice:
    path: str = ""
    count: int = 0
    marked: bool = False


@dataclass_json
@dataclass
class Netabare:
    netabare: bool = False
    display_content: bool = False
    display_comment: bool = False
    is_clicked: bool = False


@dataclass
class NetabareDisplaySetting:
    should_display_icon: bool = False
    should_gray_out_review: bool = False
    should_display_comment: bool = False


@dataclass_json
@dataclass
class CommentsResource:
    id: int
    path: str
    content_tag: str
    created_at: str
    deletable: bool
    user: User
    nice: Nice


@dataclass_json
@dataclass
class Comments:
    path: str
    metadata: Metadata
    resources: List[CommentsResource]


@dataclass_json
@dataclass
class Contents:
    image_url: Optional[str] = None


@dataclass_json
@dataclass
class ReviewResource:
    id: int
    path: str
    deletable: bool
    content_tag: str
    content: str
    created_at: str
    highlight: bool
    newly: bool
    contents: Contents
    user: User
    nice: Nice
    netabare: Netabare
    netabare_display_setting: NetabareDisplaySetting
    comments: Comments


@dataclass_json
@dataclass
class ReviewListResponse:
    metadata: Metadata
    resources: List[ReviewResource]


# ---------- Author ----------

@dataclass_json
@dataclass
class Role:
    id: int = 0
    name: str = ""


@dataclass_json
@dataclass
class Author:
    path: str = ""
    id: Optional[int] = None
    name: str = ""
    profile: Optional[str] = None
    awards: Optional[str] = None


@dataclass_json
@dataclass
class AuthorAndRole:
    author: Author
    role: Role


@dataclass_json
@dataclass
class AmazonUrls:
    outline: str = ""
    registration: str = ""
    wish_book: str = ""


@dataclass_json
@dataclass
class AuthorResource:
    id: int
    path: str
    title: str
    image_url: str
    registration_count: int
    page: int
    original: bool
    is_advertisable: bool
    published_at: str
    author: Author
    author_and_roles: List[AuthorAndRole] = ()
    read_book_count: int = 0
    amazon_urls: AmazonUrls = None


@dataclass_json
@dataclass
class AuthorResponse:
    metadata: Metadata
    title: str
    more_path: Optional[str]
    resources: List[AuthorResource]


# ---------- Book ----------
@dataclass
class Review:
    book_id: int
    review: str = ""
    source: str = "bookmeter"


@dataclass
class Book:
    id: int
    title: str
    author: str
    url: str
    published_at: datetime
    image_url: str
    page: int
    registration_count: int
    reviews: list[str]
