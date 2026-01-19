from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
from src.scrapers.base import BaseScraper

class IkmanJobsScraper(BaseScraper):
    BASE_URL = "https://ikman.lk"
    LISTING_URL = f"{BASE_URL}/en/jobs"

    # Categories to scrape
    CATEGORIES = [
        "it-web-development",
        "accounting-finance",
        "marketing",
        "engineering",
        "sales",
    ]
    
    def __init__(self):
        super().__init__("ikman")

    def scrape(self) -> List[Dict[str, Any]]:
        all_jobs = []

        for category in self.CATEGORIES:
            try:
                jobs = self._scrape_category(category)
                all_jobs.extend(jobs)
                self.logger.info(f"Category {category}: found {len(jobs)} jobs")
            except Exception as e:
                self.logger.error(f"Error scraping category {category}: {e}")

        return all_jobs

    def _scrape_category(self, category: str, max_pages: int = 3) -> List[Dict[str, Any]]:
        jobs = []

        for page in range(1, max_pages + 1):
            url = f"{self.LISTING_URL}/{category}?page={page}"
            response = self.http.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            page_jobs = self._parse_listing_page(soup, category)
            if not page_jobs:
                break

            jobs.extend(page_jobs)

        return jobs

    def _parse_listing_page(self, soup: BeautifulSoup, category: str) -> List[Dict[str, Any]]:
        jobs = []
        listings = soup.select("li[class*='item']")

        for listing in listings:
            try:
                job = self._parse_listing(listing, category)
                if job:
                    jobs.append(job)
            except Exception as e:
                self.logger.debug(f"Error parsing listing: {e}")

        return jobs

    def _parse_listing(self, listing, category: str) -> Dict[str, Any] | None:
        title_elem = listing.select_one("h2, h3, [class*='title']")
        link_elem = listing.select_one("a")
        location_elem = listing.select_one("[class*='location']")

        if not title_elem or not link_elem:
            return None

        job_url = link_elem.get("href", "")

        return {
            "source": "ikman",
            "source_job_id": self._extract_job_id(job_url),
            "title": title_elem.get_text(strip=True),
            "company": "",  # Ikman doesn't always show company
            "location": location_elem.get_text(strip=True) if location_elem else "",
            "job_url": f"{self.BASE_URL}{job_url}" if job_url.startswith("/") else job_url,
            "category": category,
            "scraped_at": self.scraped_at.isoformat(),
        }

    def _extract_job_id(self, url: str) -> str:
        match = re.search(r"-(\d+)\.html", url)
        return match.group(1) if match else url

if __name__ == "__main__":
    scraper = IkmanJobsScraper()
    scraper.run()