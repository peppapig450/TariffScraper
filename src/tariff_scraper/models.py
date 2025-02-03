from enum import StrEnum, auto
from dataclasses import dataclass

class Country(StrEnum):
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
    headers: dict[str, str] | None = None