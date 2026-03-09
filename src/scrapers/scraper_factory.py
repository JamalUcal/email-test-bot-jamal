"""
Factory for creating scraper instances based on configuration.

Handles dynamic loading of supplier-specific scrapers and provides
a unified interface for creating scraper instances.
"""

import importlib
import os
from typing import Dict, Any, Optional, Type, List, cast
from utils.logger import get_logger
from .scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from .browser_manager import BrowserManager

logger = get_logger(__name__)


class ScraperFactory:
    """
    Factory for creating scraper instances.
    
    Supports both config-driven scrapers (simple sites) and custom
    scraper classes (complex sites requiring custom logic).
    """
    
    def __init__(self, browser_manager: BrowserManager):
        """
        Initialize the factory.
        
        Args:
            browser_manager: BrowserManager instance to use
        """
        self.browser_manager = browser_manager
        self._custom_scrapers: Dict[str, Type[BaseScraper]] = {}
        
        # Load custom scrapers from supplier_scrapers module
        self._load_custom_scrapers()
    
    def _load_custom_scrapers(self) -> None:
        """Load custom scraper classes from supplier_scrapers module."""
        try:
            from . import supplier_scrapers
            
            # Get all modules in supplier_scrapers
            scraper_dir = os.path.dirname(supplier_scrapers.__file__)
            
            for filename in os.listdir(scraper_dir):
                if filename.endswith('.py') and not filename.startswith('__'):
                    module_name = filename[:-3]  # Remove .py extension
                    
                    try:
                        module = importlib.import_module(f'.supplier_scrapers.{module_name}', package='scrapers')
                        
                        # Look for scraper classes (classes ending with 'Scraper')
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (isinstance(attr, type) and 
                                issubclass(attr, BaseScraper) and 
                                attr != BaseScraper and
                                attr_name.endswith('Scraper')):
                                
                                # Extract supplier name from class name
                                supplier_name = attr_name.replace('Scraper', '').upper()
                                self._custom_scrapers[supplier_name] = attr
                                
                                logger.debug(f"Loaded custom scraper: {attr_name} for supplier {supplier_name}")
                    
                    except Exception as e:
                        logger.warning(f"Failed to load scraper module {module_name}: {str(e)}")
        
        except Exception as e:
            logger.warning(f"Failed to load custom scrapers: {str(e)}")
    
    def create_scraper(
        self,
        scraper_config: Dict[str, Any],
        core_config: Optional[Dict[str, Any]] = None,
        service_account_info: Optional[Dict[str, Any]] = None,
        start_index: int = 0,
        state_manager: Optional[Any] = None
    ) -> Optional[BaseScraper]:
        """
        Create a scraper instance based on configuration.
        
        Args:
            scraper_config: Supplier scraping configuration
            core_config: Core configuration (optional, for GCP deployment)
            service_account_info: Service account info (optional, for GCP deployment)
            start_index: Index to resume from (for interrupted runs, 0-based)
            
        Returns:
            Scraper instance or None if creation failed
        """
        try:
            supplier_name = scraper_config['supplier']
            
            # Check if we have a custom scraper for this supplier
            if supplier_name in self._custom_scrapers:
                scraper_class = self._custom_scrapers[supplier_name]
                logger.info(f"Creating custom scraper for {supplier_name}")
                return scraper_class(scraper_config, self.browser_manager, start_index, state_manager)
            
            # Check if config specifies a custom scraper class
            custom_class_name = scraper_config.get('custom_scraper_class')
            if custom_class_name:
                try:
                    scraper_class = self._get_scraper_class_by_name(custom_class_name)
                    logger.info(f"Creating custom scraper {custom_class_name} for {supplier_name}")
                    return scraper_class(scraper_config, self.browser_manager, start_index, state_manager)
                except Exception as e:
                    logger.error(f"Failed to create custom scraper {custom_class_name}: {str(e)}")
                    return None
            
            # Route by site type if provided
            scraper_type = scraper_config.get('type')
            if scraper_type == 'link_downloader':
                from .templates.link_downloader_scraper import LinkDownloaderScraper
                logger.info(f"Creating LinkDownloaderScraper for {supplier_name}")
                return LinkDownloaderScraper(
                    browser_manager=self.browser_manager,
                    scraper_config=scraper_config,
                    core_config=core_config or {},
                    service_account_info=service_account_info or {},
                    start_index=start_index,
                    state_manager=state_manager
                )

            # Default to config-driven scraper
            logger.info(f"Creating config-driven scraper for {supplier_name}")
            return ConfigDrivenScraper(scraper_config, self.browser_manager, start_index, state_manager)
            
        except Exception as e:
            logger.error(f"Failed to create scraper for {scraper_config.get('supplier', 'unknown')}: {str(e)}")
            return None
    
    def _get_scraper_class_by_name(self, class_name: str) -> Type[BaseScraper]:
        """
        Get a scraper class by its fully qualified name.
        
        Args:
            class_name: Fully qualified class name (e.g., 'scrapers.supplier_scrapers.example.ExampleScraper')
            
        Returns:
            Scraper class
        """
        try:
            module_path, class_name_only = class_name.rsplit('.', 1)
            module = importlib.import_module(module_path)
            return cast(Type[BaseScraper], getattr(module, class_name_only))
        except Exception as e:
            raise ValueError(f"Cannot load scraper class {class_name}: {str(e)}")


class ConfigDrivenScraper(BaseScraper):
    """
    Config-driven scraper for simple supplier websites.
    
    Uses configuration to drive authentication, navigation, and downloading
    without requiring custom code for each supplier.
    """
    
    async def scrape(self) -> ScrapingResult:
        """Perform config-driven scraping workflow."""
        import time
        
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            logger.info(f"Starting config-driven scraping for {self.supplier_name}")
            
            # Step 1: Authenticate
            auth_success = await self.authenticate()
            if not auth_success:
                result.errors.append("Authentication failed")
                return result
            
            # Step 2: Navigate to downloads
            nav_success = await self.navigate_to_downloads()
            if not nav_success:
                result.errors.append("Navigation to downloads failed")
                return result
            
            # Step 3: Download files
            files = await self.download_files()
            result.files = files
            
            if files:
                result.success = True
                logger.info(f"Successfully scraped {len(files)} files for {self.supplier_name}")
            else:
                result.errors.append("No files downloaded")
            
        except Exception as e:
            logger.error(f"Scraping failed for {self.supplier_name}: {str(e)}")
            result.errors.append(f"Scraping error: {str(e)}")
            await self.browser_manager.take_screenshot(f"scraping_error_{self.supplier_name}")
        
        finally:
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    async def authenticate(self) -> bool:
        """Authenticate using configuration."""
        auth_config = self.config.get('authentication', {})
        
        if not auth_config:
            logger.info(f"No authentication required for {self.supplier_name}")
            return True
        
        method = auth_config.get('method', 'form')
        login_url = auth_config.get('login_url')
        
        if not login_url:
            logger.error(f"No login_url configured for {self.supplier_name}")
            return False
        
        # Navigate to login page
        if not await self.browser_manager.navigate(login_url):
            return False
        
        if method == 'form':
            return await self._authenticate_form(auth_config)
        else:
            logger.error(f"Unsupported authentication method: {method}")
            return False
    
    async def _authenticate_form(self, auth_config: Dict[str, Any]) -> bool:
        """Handle form-based authentication."""
        # Get credentials from environment or config
        username = self._get_credential('username', auth_config)
        password = self._get_credential('password', auth_config)
        
        if not username or not password:
            logger.error(f"Missing credentials for {self.supplier_name}")
            return False
        
        username_field = auth_config.get('username_field')
        password_field = auth_config.get('password_field')
        submit_button = auth_config.get('submit_button')
        
        if not username_field or not password_field:
            logger.error(f"Missing form field selectors for {self.supplier_name}")
            return False
        
        return await self.browser_manager.fill_form(
            username_field=username_field,
            password_field=password_field,
            username=username,
            password=password,
            submit_button=submit_button
        )
    
    async def navigate_to_downloads(self) -> bool:
        """Navigate to downloads page using configuration."""
        navigation_steps = self.config.get('navigation', [])
        
        for step in navigation_steps:
            action = step.get('action')
            selector = step.get('selector')
            wait_for = step.get('wait_for', 'load')
            
            if action == 'click' and selector:
                if not await self.browser_manager.click_and_wait(selector, wait_for):
                    logger.error(f"Navigation step failed: {step}")
                    return False
            else:
                logger.warning(f"Unsupported navigation action: {action}")
        
        return True
    
    async def download_files(self) -> List[ScrapedFile]:
        """Download files using configuration."""
        download_config = self.config.get('download', {})
        method = download_config.get('method', 'click')
        selector = download_config.get('selector')
        
        if not selector:
            logger.error(f"No download selector configured for {self.supplier_name}")
            return []
        
        files = []
        
        if method == 'click':
            file_path = await self.browser_manager.download_file(selector)
            if file_path:
                filename = os.path.basename(file_path)
                scraped_file = self.create_scraped_file(filename, file_path)
                files.append(scraped_file)
        
        return files
    
    def _get_credential(self, credential_type: str, auth_config: Dict[str, Any]) -> Optional[str]:
        """Get credential from environment or config."""
        # Try environment variable first
        env_key = f"SCRAPER_{self.supplier_name}_{credential_type.upper()}"
        credential = os.getenv(env_key)
        
        if credential:
            return credential
        
        # Try config fallback
        return auth_config.get(credential_type)
