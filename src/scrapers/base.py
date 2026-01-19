from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime
import json
from pathlib import Path
from src.config import settings
from src.utils.logger import get_logger
from src.utils.http_client import http_client

class BaseScraper(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"scraper.{name}")
        self.http = http_client
        self.scraped_at = datetime.utcnow()
        
    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """Scrape data from source. Returns list of raw records."""
        pass
    
    def save_raw(self, data: List[Dict[str, Any]]) -> Path:
        """Save raw scraped data to JSON file."""
        date_str = self.scraped_at.strftime("%Y-%m-%d")
        output_dir = settings.RAW_DIR / self.name / date_str
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"{self.name}_{self.scraped_at.strftime('%H%M%S')}.json"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({
                "source": self.name,
                "scraped_at": self.scraped_at.isoformat(),
                "record_count": len(data),
                "records": data
            }, f, ensure_ascii=False, indent=2)

        self.logger.info(f"Saved {len(data)} records to {output_file}")
        return output_file
    
    def run(self) -> Path:
        """Execute scraping and save results."""
        self.logger.info(f"Starting scraper: {self.name}")
        data = self.scrape()
        output_path = self.save_raw(data)
        self.logger.info(f"Completed scraper: {self.name} - {len(data)} records")
        return output_path
