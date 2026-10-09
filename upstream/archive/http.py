"""A polite HTTP client: one User-Agent, a per-host rate limit shared by all threads, retries with backoff.

429 and 5xx answers and network errors are retried (Retry-After is honoured); after `tries` attempts the
last error is raised. 404 is not an error here: get() returns the response and the caller decides.
"""
from __future__ import annotations

import random
import threading
import time
from urllib.parse import urlsplit

import requests

from . import settings

RETRY_STATUS = {429, 500, 502, 503, 504, 520, 521, 522, 524}


def host_key(url: str) -> str:
    host = urlsplit(url).hostname or ""
    # WikiArt spreads images over uploads0..uploads8: one budget for all of them
    if host.startswith("uploads") and host.endswith(".wikiart.org"):
        return "uploads0.wikiart.org"
    return host


class RateLimiter:
    """At most `rate` request starts per second (evenly spaced), across threads."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at)
            self.next_at = start + self.interval
        if start > now:
            time.sleep(start - now)

    def pause(self, seconds: float) -> None:
        """Everyone waits: the host asked us to slow down."""
        with self.lock:
            self.next_at = max(self.next_at, time.monotonic() + seconds)


class Client:
    def __init__(self, rates: dict[str, float] | None = None, timeout: float = 60, tries: int = 6):
        self.rates = {**settings.RATE, **(rates or {})}
        self.limiters: dict[str, RateLimiter] = {}
        self.lock = threading.Lock()
        self.timeout = timeout
        self.tries = tries
        self.local = threading.local()

    def session(self) -> requests.Session:
        s = getattr(self.local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers["User-Agent"] = settings.USER_AGENT
            adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=16)
            s.mount("https://", adapter)
            self.local.session = s
        return s

    def limiter(self, url: str) -> RateLimiter:
        key = host_key(url)
        with self.lock:
            if key not in self.limiters:
                self.limiters[key] = RateLimiter(self.rates.get(key, settings.DEFAULT_RATE))
            return self.limiters[key]

    def get(self, url: str, **kw) -> requests.Response:
        limiter = self.limiter(url)
        error: Exception | None = None
        for attempt in range(self.tries):
            limiter.wait()
            try:
                r = self.session().get(url, timeout=self.timeout, **kw)
            except requests.RequestException as exc:
                error = exc
                time.sleep(min(120, 2 ** attempt + random.random()))
                continue
            if r.status_code in RETRY_STATUS:
                error = requests.HTTPError(f"HTTP {r.status_code} for {url}", response=r)
                retry_after = r.headers.get("Retry-After", "")
                wait = float(retry_after) if retry_after.isdigit() else min(300, 5 * 2 ** attempt)
                limiter.pause(wait)
                time.sleep(wait)
                continue
            return r
        raise error or RuntimeError(f"GET {url} failed")

    def json(self, url: str, **kw):
        """The JSON body, or None for 404. Other non-2xx answers raise."""
        r = self.get(url, **kw)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
