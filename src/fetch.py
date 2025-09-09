import time
import requests
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Cache HTTP responses on disk for 24h to avoid hammering the site
requests_cache.install_cache("http_cache", backend="sqlite", expire_after=60*60*24)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "pfr-scraper (educational; contact: your_email@example.com)"
})

LAST_CALL = [0.0]  # mutable container for closure

class FetchError(Exception): ...

def _throttle(min_interval=1.0):
    """Enforce ~1 request/sec when not served from cache."""
    now = time.time()
    delay = min_interval - (now - LAST_CALL[0])
    if delay > 0:
        time.sleep(delay)
    LAST_CALL[0] = time.time()

@retry(
    retry=retry_if_exception_type(FetchError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8)
)
def get(url: str, *, timeout=30) -> str:
    resp = SESSION.get(url, timeout=timeout)
    if not getattr(resp, "from_cache", False):
        _throttle(1.0)
    if resp.status_code != 200 or not resp.text:
        raise FetchError(f"Bad status {resp.status_code} for {url}")
    return resp.text
