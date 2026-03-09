"""
Base scraper interface and data structures.

Defines the abstract base class that all supplier scrapers must implement,
along with data structures for representing scraped files and results.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, TYPE_CHECKING
from datetime import datetime
from pathlib import Path

if TYPE_CHECKING:
    from .browser_manager import BrowserManager  # for type checking only


@dataclass
class ScrapedFile:
    """Represents a file downloaded from a supplier website."""
    filename: str  # Normalized filename for our system
    local_path: str
    supplier: str
    brand: Optional[str] = None  # Raw brand from supplier (e.g., "VAG-OIL", "BMW_PART1")
    location: Optional[str] = None
    currency: Optional[str] = None
    expiry_date: Optional[datetime] = None  # Legacy, for parsed dates
    valid_from_date: Optional[datetime] = None  # Legacy, for parsed dates
    valid_from_date_str: Optional[str] = None  # Raw date string from supplier
    expiry_date_str: Optional[str] = None  # Raw date string from supplier
    supplier_filename: Optional[str] = None  # Original filename from supplier's website (for duplicate detection)
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class ScrapingResult:
    """Result of scraping a supplier website."""
    supplier: str
    success: bool
    files: List[ScrapedFile] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    execution_time_seconds: float = 0.0
    screenshots: List[str] = field(default_factory=list)  # Paths to error screenshots
    total_files_found: int = 0  # Total files found before duplicate filtering
    files_skipped_duplicates: int = 0  # Files skipped due to duplicate detection


class BaseScraper(ABC):
    """
    Abstract base class for supplier website scrapers.
    
    Each supplier that requires custom scraping logic should implement
    this interface. The scraper handles authentication, navigation,
    and file downloading specific to that supplier's website.
    """
    
    def __init__(self, config: Dict[str, Any], browser_manager: 'BrowserManager', start_index: int = 0, state_manager: Optional[Any] = None):
        """
        Initialize the scraper.
        
        Args:
            config: Supplier-specific scraping configuration
            browser_manager: BrowserManager instance for automation
            start_index: Index to resume from (for interrupted runs, 0-based)
            state_manager: Optional StateManager for duplicate detection
        """
        self.config = config
        self.browser_manager = browser_manager
        self.supplier_name = config['supplier']
        self.start_index = start_index
        self.state_manager = state_manager
        
    @abstractmethod
    async def scrape(self) -> ScrapingResult:
        """
        Perform the complete scraping workflow.
        
        This method should:
        1. Authenticate with the supplier website
        2. Navigate to the appropriate download page
        3. Download price list files
        4. Extract metadata (brand, expiry date, etc.)
        5. Return ScrapingResult with downloaded files
        
        Returns:
            ScrapingResult containing downloaded files and metadata
        """
        pass
    
    @abstractmethod
    async def authenticate(self) -> bool:
        """
        Authenticate with the supplier website.
        
        Returns:
            True if authentication successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def navigate_to_downloads(self) -> bool:
        """
        Navigate to the downloads/download page.
        
        Returns:
            True if navigation successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def download_files(self) -> List[ScrapedFile]:
        """
        Download price list files from the current page.
        
        Returns:
            List of ScrapedFile objects representing downloaded files
        """
        pass
    
    def get_metadata_from_config(self) -> Dict[str, Any]:
        """
        Extract metadata from scraper configuration.
        
        Returns:
            Dictionary containing brand, location, currency, etc.
        """
        return {
            'brand': self.config.get('brand'),
            'location': self.config.get('location'),
            'currency': self.config.get('currency'),
            'default_expiry_days': self.config.get('default_expiry_days', 90)
        }
    
    def create_scraped_file(
        self,
        filename: str,
        local_path: str,
        **kwargs
    ) -> ScrapedFile:
        """
        Create a ScrapedFile object with default metadata from config.
        
        Args:
            filename: Name of the downloaded file
            local_path: Local path where file was saved
            **kwargs: Additional metadata to override defaults
            
        Returns:
            ScrapedFile object
        """
        from datetime import datetime, timezone, timedelta
        
        metadata = self.get_metadata_from_config()
        metadata.update(kwargs)
        
        # Calculate expiry_date from default_expiry_days if not explicitly provided
        if 'expiry_date' not in metadata and 'default_expiry_days' in metadata:
            default_days = metadata.pop('default_expiry_days')
            if default_days and not metadata.get('expiry_date'):
                metadata['expiry_date'] = datetime.now(timezone.utc) + timedelta(days=default_days)
        elif 'default_expiry_days' in metadata:
            # Remove default_expiry_days if expiry_date is already set
            metadata.pop('default_expiry_days')
        
        return ScrapedFile(
            filename=filename,
            local_path=local_path,
            supplier=self.supplier_name,
            **metadata
        )

