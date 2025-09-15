import os
import time
import random
import requests
import requests_cache

requests_cache.install_cache("http_cache", backend="sqlite", expire_after=60 * 60 * 24)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "pfr-scraper (research / non-commercial; contact: you@example.com)"
})

class FetchError(Exception): ...
class RateLimited(FetchError): ...

# Policy: "skip" (skip 429s) or "backoff" (short sleep then retry)
PFR_POLICY = os.getenv("PFR_POLICY", "skip").lower()
PFR_MIN_INTERVAL = float(os.getenv("PFR_MIN_INTERVAL", "0.0"))

_last_noncached_at = [0.0]

def _throttle():
    if PFR_MIN_INTERVAL <= 0:
        return
    now = time.time()
    elapsed = now - _last_noncached_at[0]
    remaining = PFR_MIN_INTERVAL - elapsed
    if remaining > 0:
        time.sleep(remaining)
    _last_noncached_at[0] = time.time()

def get(url: str, *, timeout=20) -> str:
    resp = SESSION.get(url, timeout=timeout)

    if not getattr(resp, "from_cache", False):
        _throttle()

    if resp.status_code == 429:
        if PFR_POLICY == "backoff":
            retry_after = resp.headers.get("Retry-After")
            try:
                wait_s = min(float(retry_after), 20.0) if retry_after else 10.0
            except (TypeError, ValueError):
                wait_s = 10.0
            time.sleep(wait_s + random.uniform(0.2, 0.8))
            raise RateLimited(f"429 (backoff {wait_s}s) for {url}")
        else:
            raise RateLimited(f"429 skip for {url}")

    if resp.status_code != 200 or not resp.text:
        raise FetchError(f"Bad status {resp.status_code} for {url}")

    return resp.text
