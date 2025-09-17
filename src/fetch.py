# src/fetch.py
import os
import time
import random
from typing import Optional, Tuple
import requests
import requests_cache

# Cache to reduce load and make reruns fast
CACHE_SECS = int(os.getenv("PFR_CACHE_SECS", "86400"))  # 24h
requests_cache.install_cache("http_cache", backend="sqlite", expire_after=CACHE_SECS)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "pfr-scraper/1.0 (research; contact: you@example.com)"
})

# Tunables (env overrides)
PFR_MIN_INTERVAL = float(os.getenv("PFR_MIN_INTERVAL", "2.0"))          # seconds between requests
PFR_MAX_ATTEMPTS = int(os.getenv("PFR_MAX_ATTEMPTS", "5"))              # tries per URL
PFR_CONNECT_TIMEOUT = float(os.getenv("PFR_CONNECT_TIMEOUT", "6.0"))
PFR_READ_TIMEOUT = float(os.getenv("PFR_READ_TIMEOUT", "15.0"))
PFR_RETRY_AFTER_CAP = float(os.getenv("PFR_RETRY_AFTER_CAP", "45.0"))   # cap Retry-After

_last_request_ts = 0.0

class FetchError(Exception):
    pass

def _sleep_min_interval():
    """Throttle between requests; adds a smidge of jitter."""
    global _last_request_ts
    now = time.time()
    elapsed = now - _last_request_ts
    wait_for = PFR_MIN_INTERVAL - elapsed
    if wait_for > 0:
        time.sleep(wait_for + random.uniform(0.05, 0.25))
    _last_request_ts = time.time()

def _retry_after_seconds(resp: requests.Response) -> float:
    ra = resp.headers.get("Retry-After")
    if ra is None:
        return min(PFR_RETRY_AFTER_CAP, 10.0)
    try:
        sec = float(ra)
        return min(PFR_RETRY_AFTER_CAP, max(0.0, sec))
    except ValueError:
        return min(PFR_RETRY_AFTER_CAP, 10.0)

def _backoff_delay(attempt: int) -> float:
    # Exponential backoff with jitter, bounded
    base = min(8.0 * (2 ** (attempt - 1)), 30.0)
    return base + random.uniform(0.1, 0.6)

def get(url: str, *, timeout: Optional[Tuple[float, float]] = None) -> str:
    """
    Polite GET with:
      - pre-request throttle
      - honoring Retry-After on 429 (capped)
      - exponential backoff on transient errors
      - limited retries
      - requests-cache for reruns
    """
    if timeout is None:
        timeout = (PFR_CONNECT_TIMEOUT, PFR_READ_TIMEOUT)

    for attempt in range(1, PFR_MAX_ATTEMPTS + 1):
        _sleep_min_interval()
        try:
            resp = SESSION.get(url, timeout=timeout)
        except requests.RequestException as e:
            if attempt >= PFR_MAX_ATTEMPTS:
                raise FetchError(f"Request error for {url}: {e}") from e
            time.sleep(_backoff_delay(attempt))
            continue

        status = resp.status_code

        if status == 200 and resp.text:
            return resp.text

        if status == 429:
            wait_s = _retry_after_seconds(resp)
            if attempt >= PFR_MAX_ATTEMPTS:
                raise FetchError(f"429 Too Many Requests (gave up after {attempt}) for {url}")
            time.sleep(wait_s + random.uniform(0.2, 0.8))
            continue

        if status >= 500 or status in (408,):
            if attempt >= PFR_MAX_ATTEMPTS:
                raise FetchError(f"Server error {status} after {attempt} attempts for {url}")
            time.sleep(_backoff_delay(attempt))
            continue

        raise FetchError(f"Bad status {status} for {url}")

    raise FetchError(f"Failed to fetch {url} after {PFR_MAX_ATTEMPTS} attempts")
