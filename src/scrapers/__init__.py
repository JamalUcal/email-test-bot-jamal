"""
Website scraping module for downloading price list files from supplier portals.

This module provides:
- Base scraper interface for implementing supplier-specific scrapers
- Browser automation management via Playwright
- Config-driven scraping with custom scraper support
- Integration with existing file parsing pipeline
"""

from .scraper_base import BaseScraper, ScrapedFile
from .browser_manager import BrowserManager
from .scraper_factory import ScraperFactory
from .templates.link_downloader_scraper import LinkDownloaderScraper

__all__ = [
    'BaseScraper',
    'ScrapedFile',
    'BrowserManager',
    'ScraperFactory',
    'LinkDownloaderScraper',
]



