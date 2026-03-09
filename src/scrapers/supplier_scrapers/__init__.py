"""
Supplier-specific scraper implementations.

Each supplier that requires custom scraping logic should have its own
scraper class in this directory, inheriting from BaseScraper.

Naming convention: {supplier_name}_scraper.py with class {SupplierName}Scraper
Example: apf_scraper.py with class ApfScraper

The scraper_factory will automatically discover scrapers in this directory
by matching supplier name to file/class name.
"""

from typing import List

from scrapers.supplier_scrapers.apf_scraper import ApfScraper
from scrapers.supplier_scrapers.neoparta_scraper import NeopartaScraper

__all__: List[str] = [
    "ApfScraper",
    "NeopartaScraper",
]



