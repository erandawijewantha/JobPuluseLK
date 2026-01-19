import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from fake_useragent import UserAgent
import time
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)
ua = UserAgent()

class HTTPClient:
    def __init__(self):
        self.session = requests.Session()
        self.last_request_time = 0
    
    def _get_headers(self):
        return {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }
        
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < settings.REQUEST_DELAY:
            time.sleep(settings.REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    
    def get(self, url: str, **kwargs) -> requests.Response:
        self._rate_limit()
        headers = {**self._get_headers(), **kwargs.pop("headers", {})}

        logger.debug(f"GET {url}")
        response = self.session.get(url, headers=headers, timeout=30, **kwargs)
        response.raise_for_status()

        return response
    
http_client = HTTPClient()
