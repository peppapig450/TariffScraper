from bs4 import BeautifulSoup, Tag
import logging
from .base import BaseTariffScraper

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