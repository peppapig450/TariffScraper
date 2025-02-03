from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
import json
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime
from requests.exceptions import RequestException
from dataclasses import dataclass
from enum import Enum, auto

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tariff_scraper.log'),
        logging.StreamHandler()
    ]
)

class Country(Enum):
    """Supported countries for tariff data."""
    CANADA = auto()
    MEXICO = auto()
    CHINA = auto()

@dataclass
class ScraperConfig:
    """Configuration for a tariff scraper."""
    url: str
    country: Country
    language: str
    encoding: str = 'utf-8'
    headers: Optional[Dict[str, str]] = None

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (pd.Timestamp, datetime)):
            return o.isoformat()
        return super().default(o)
    
class TariffData:
    """Class to store and manipulate tariff data."""
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        
    def add_data(self, data: List[Dict[str, Any]], country: Country):
        """Add new data to the DataFrame with country information."""
        new_df = pd.DataFrame(data)
        new_df['Country'] = country.name
        new_df['Scrape_Date'] = datetime.now()
        
        if self.df is None:
            self.df = new_df
        else:
            self.df = pd.concat([self.df, new_df], ignore_index=True)

    def get_statistics(self) -> dict:
        """Get statistics about the tariff data."""
        if self.df is None:
            raise ValueError("No data available")

        stats = {
            "total_entries": len(self.df),
            "entries_by_country": self.df.groupby('Country').size().to_dict(),
            "unique_hs_headings": self.df['HS Heading'].nunique(),
            "unique_tariff_items": self.df['Tariff Item'].nunique(),
        }
        
        # Add country-specific statistics
        for country in self.df['Country'].unique():
            country_df = self.df[self.df['Country'] == country]
            stats[f"{country.lower()}_statistics"] = {
                "total_entries": len(country_df),
                "unique_hs_headings": country_df['HS Heading'].nunique(),
                "avg_descriptions_per_heading": country_df.groupby('HS Heading').size().mean()
            }
            
        return stats

class BaseTariffScraper(ABC):
    """Abstract base class for tariff scrapers."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.data = []

    def fetch_page(self) -> Optional[str]:
        """Fetch webpage content with error handling."""
        try:
            response = requests.get(
                self.config.url,
                headers=self.config.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.text
        except RequestException as e:
            logging.error(f"Failed to fetch webpage for {self.config.country}: {str(e)}")
            return None

    @abstractmethod
    def parse_data(self, html_content: str) -> bool:
        """Parse the HTML content into structured data."""
        pass

    def scrape(self) -> List[Dict[str, Any]]:
        """Execute the scraping process."""
        html_content = self.fetch_page()
        if html_content and self.parse_data(html_content):
            return self.data
        return []

class CanadianTariffScraper(BaseTariffScraper):
    """Scraper for Canadian tariff data."""
    
    def parse_description(self, cell: Tag) -> str:
        list_items = cell.find_all('li')
        if list_items:
            return "; ".join(li.get_text(" ", strip=True) for li in list_items)
        return cell.get_text(" ", strip=True)

    def parse_data(self, html_content: str) -> bool:
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table')
        
        if not table:
            logging.error("No table found in Canadian tariff page")
            return False

        tbody = table.find('tbody')
        if not isinstance(tbody, Tag):
            logging.error("No valid table body found")
            return False

        for row in tbody.find_all('tr'):
            try:
                tariff_item = row.find('th').get_text(strip=True)
                cells = row.find_all('td')
                
                if len(cells) < 2:
                    continue

                self.data.append({
                    'Tariff Item': tariff_item,
                    'HS Heading': cells[0].get_text(" ", strip=True),
                    'Description': self.parse_description(cells[1])
                })

            except AttributeError as e:
                logging.error(f"Error processing row: {str(e)}")
                continue

        return bool(self.data)

class ChineseTariffScraper(BaseTariffScraper):
    """Scraper for Chinese tariff data."""
    
    def parse_data(self, html_content: str) -> bool:
        # Implement Chinese-specific parsing logic
        # This is a placeholder - implement actual logic when adding Chinese tariff support
        logging.info("Chinese tariff parsing not yet implemented")
        return False

class MexicanTariffScraper(BaseTariffScraper):
    """Scraper for Mexican tariff data."""
    
    def parse_data(self, html_content: str) -> bool:
        # Implement Mexican-specific parsing logic
        # This is a placeholder - implement actual logic when adding Mexican tariff support
        logging.info("Mexican tariff parsing not yet implemented")
        return False

class TariffScraperFactory:
    """Factory class to create appropriate scraper instances."""
    
    @staticmethod
    def create_scraper(config: ScraperConfig) -> BaseTariffScraper:
        scrapers = {
            Country.CANADA: CanadianTariffScraper,
            Country.MEXICO: MexicanTariffScraper,
            Country.CHINA: ChineseTariffScraper
        }
        
        scraper_class = scrapers.get(config.country)
        if not scraper_class:
            raise ValueError(f"No scraper available for {config.country}")
        
        return scraper_class(config)

class TariffManager:
    """Main class to manage tariff data collection and storage."""
    
    def __init__(self, output_dir: str = "tariff_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.tariff_data = TariffData()

    def scrape_country(self, config: ScraperConfig) -> bool:
        """Scrape tariff data for a specific country."""
        try:
            scraper = TariffScraperFactory.create_scraper(config)
            data = scraper.scrape()
            
            if data:
                self.tariff_data.add_data(data, config.country)
                return True
            return False
            
        except Exception as e:
            logging.error(f"Error scraping {config.country}: {str(e)}")
            return False

    def save_data(self) -> bool:
        """Save all collected data to files."""
        try:
            if self.tariff_data.df is None:
                raise ValueError("No data available to save")

            # Save to JSON
            json_file = self.output_dir / "combined_tariff_data.json"
            output_dict = {
                "metadata": {
                    "last_updated": datetime.now().isoformat(),
                    "statistics": self.tariff_data.get_statistics()
                },
                "tariffs": self.tariff_data.df.to_dict('records')
            }
            
            with open(json_file, "w", encoding='utf-8') as f:
                json.dump(output_dict, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)

            # Save to Excel with multiple sheets
            excel_file = self.output_dir / "combined_tariff_data.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                # Save combined data
                self.tariff_data.df.to_excel(writer, sheet_name='All_Data', index=False)
                
                # Save country-specific sheets
                for country in self.tariff_data.df['Country'].unique():
                    country_df = self.tariff_data.df[self.tariff_data.df['Country'] == country]
                    country_df.to_excel(writer, sheet_name=f'{country}_Data', index=False)

            logging.info(f"Data saved to {json_file} and {excel_file}")
            return True

        except Exception as e:
            logging.error(f"Error saving data: {str(e)}")
            return False

def main():
    # Example usage
    manager = TariffManager()
    
    # Configure scrapers for different countries
    configs = [
        ScraperConfig(
            url="https://www.canada.ca/en/department-finance/news/2025/02/list-of-products-from-the-united-states-subject-to-25-per-cent-tariffs-effective-february-4-2025.html",
            country=Country.CANADA,
            language="en",
            headers={"User-Agent": "Mozilla/5.0"}
        ),
        # Add more configs for other countries when ready
    ]
    
    # Scrape data from all configured sources
    for config in configs:
        if manager.scrape_country(config):
            logging.info(f"Successfully scraped data for {config.country}")
        else:
            logging.error(f"Failed to scrape data for {config.country}")
    
    # Save all collected data
    manager.save_data()

if __name__ == "__main__":
    main()