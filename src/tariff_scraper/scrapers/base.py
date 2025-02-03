import logging
from abc import ABC, abstractmethod
from typing import Any

import requests

from ..models import ScraperConfig


class BaseTariffScraper(ABC):
    """Abstract base class for tariff scrapers."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self.data = []

    def fetch_page(self) -> str | None:
        """Fetch webpage content with error handling."""
        try:
            response = requests.get(
                self.config.url, headers=self.config.headers, timeout=30
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(
                f"Failed to fetch webpage for {self.config.country}: {str(e)}"
            )
            return None

    @abstractmethod
    def parse_data(self, html_content: str) -> bool:
        """Parse the HTML content into structured data."""
        pass

    def scrape(self) -> list[dict[str, Any]]:
        """Execute the scraping process."""
        html_content = self.fetch_page()
        if html_content and self.parse_data(html_content):
            return self.data
        return []
