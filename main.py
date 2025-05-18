import asyncio
from config import settings
from utils.scraper import BookmeterScraper

if __name__ == "__main__":
    scraper = BookmeterScraper(settings=settings)
    asyncio.run(scraper.run())
