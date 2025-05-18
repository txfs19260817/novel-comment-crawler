import asyncio
import random
from time import sleep
from typing import Optional, Dict, Any, Self

import httpx
import requests


class HttpClient:
    """Tiny HTTP client with retries + exponential back‑off."""

    def __init__(self, retries: int = 2, base_backoff: float = 1.0):
        self._session = requests.Session()
        self._retries = retries
        self._base_backoff = base_backoff

    def get_json(self, url: str) -> dict:
        response = {"status_code": -1}
        for attempt in range(self._retries + 1):
            response = self._session.get(url, timeout=30)
            if response.ok:
                return response.json()
            self._sleep(attempt)
        raise RuntimeError(
            f"Failed to fetch {url} after {self._retries} retries (final status {response.status_code})"
        )

    def _sleep(self, attempt: int) -> None:
        sleep((2 ** attempt) * self._base_backoff + random.random())


class HttpClientAsync:
    """Tiny async HTTP client with retries and exponential back‑off."""

    def __init__(self, retries: int = 2, base_backoff: float = 1.0) -> None:
        self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self._retries = retries
        self._base_backoff = base_backoff

    # -------- public API -------- #

    async def get_json(self, url: str) -> Dict[str, Any]:
        return await self._request(url, expect_json=True)  # type: ignore[return-value]

    async def get_text(self, url: str) -> str:
        return await self._request(url, expect_json=False)  # type: ignore[return-value]

    async def aclose(self) -> None:
        await self._client.aclose()

    # -------- context‑manager sugar -------- #

    async def __aenter__(self) -> Self:  # noqa: D401
        return self

    async def __aexit__(self, *exc_info) -> Optional[bool]:
        await self.aclose()
        return None

    # -------- internals -------- #

    async def _request(self, url: str, *, expect_json: bool):
        last_exc: Exception | None = None
        for attempt in range(self._retries + 1):
            try:
                resp = await self._client.get(url)
                resp.raise_for_status()
                return resp.json() if expect_json else resp.text
            except (httpx.HTTPStatusError, httpx.TransportError, ValueError) as e:
                last_exc = e
                if attempt == self._retries:
                    break
                await self._sleep(attempt)
        raise RuntimeError(f"Failed to fetch {url} after {self._retries} retries") from last_exc

    async def _sleep(self, attempt: int) -> None:
        await asyncio.sleep((2 ** attempt) * self._base_backoff + random.random())
