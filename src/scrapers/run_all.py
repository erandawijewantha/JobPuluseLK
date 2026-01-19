# src/scrapers/run_all.py
from src.scrapers.topjobs import TopJobsScraper
from src.scrapers.ikman import IkmanJobsScraper
from src.utils.logger import get_logger

logger = get_logger("scraper.runner")

def run_all_scrapers():
    scrapers = [
        TopJobsScraper(),
        IkmanJobsScraper(),
    ]

    results = []
    for scraper in scrapers:
        try:
            output_path = scraper.run()
            results.append({"scraper": scraper.name, "status": "success", "output": str(output_path)})
        except Exception as e:
            logger.error(f"Scraper {scraper.name} failed: {e}")
            results.append({"scraper": scraper.name, "status": "failed", "error": str(e)})

    return results

if __name__ == "__main__":
    results = run_all_scrapers()
    for r in results:
        print(f"{r['scraper']}: {r['status']}")