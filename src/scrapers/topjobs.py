from typing import List, Dict, Any
from bs4 import BeautifulSoup
import re
from src.scrapers.base import BaseScraper

class TopJobsScraper(BaseScraper):
    BASE_URL = "https://www.topjobs.lk"
    LISTING_URL = f"{BASE_URL}/applicant/vacancybyfunctionalarea.jsp"
    
    # Functional areas to scrape (IT-focused for start)
    FUNCTIONAL_AREAS = [
        "1",   # IT-Software
        "2",   # IT-Hardware/Networks
        "15",  # Engineering
        "7",   # Banking/Finance
        "18",  # Marketing
    ]
    
    def __init__(self):
        super().__init__("topjobs")
        
    def scrape(self) -> List[Dict[str, Any]]:
        all_jobs = []
        
        for area_id in self.FUNCTIONAL_AREAS:
            try:
                jobs = self._scrape_area(area_id)
                all_jobs.extend(jobs)
                self.logger.info(f"Area {area_id}: found {len(jobs)} jobs")
            except Exception as e:
                self.logger.error(f"Error scraping area {area_id}: {e}")

        return all_jobs
    
    def _scrape_area(self, area_id: str) -> List[Dict[str, Any]]:
        url = f"{self.LISTING_URL}?FA={area_id}"
        response = self.http.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        jobs = []
        job_rows = soup.select("table.tb tr")

        for row in job_rows:
            try:
                job = self._parse_job_row(row, area_id)
                if job:
                    jobs.append(job)
            except Exception as e:
                self.logger.debug(f"Error parsing row: {e}")

        return jobs
    
    def _parse_job_row(self, row, area_id: str) -> Dict[str, Any] | None:
        cols = row.find_all("td")
        if len(cols) < 4:
            return None

        title_cell = cols[0]
        company_cell = cols[1]

        title_link = title_cell.find("a")
        if not title_link:
            return None

        job_url = title_link.get("href", "")
        job_id = self._extract_job_id(job_url)

        return {
            "source": "topjobs",
            "source_job_id": job_id,
            "title": title_link.get_text(strip=True),
            "company": company_cell.get_text(strip=True),
            "job_url": f"{self.BASE_URL}{job_url}" if job_url.startswith("/") else job_url,
            "functional_area_id": area_id,
            "scraped_at": self.scraped_at.isoformat(),
        }

    def _extract_job_id(self, url: str) -> str:
        match = re.search(r"VacancyID=(\d+)", url)
        return match.group(1) if match else ""

# CLI entrypoint
if __name__ == "__main__":
    scraper = TopJobsScraper()
    scraper.run()