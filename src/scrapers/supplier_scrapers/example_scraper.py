"""
Example supplier scraper implementation.

This is a template for creating custom scrapers for suppliers that require
specialized logic beyond what the config-driven scraper can handle.
"""

import os
from typing import List, Dict, Any
from datetime import datetime, timezone

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from utils.logger import get_logger
from utils.credential_manager import CredentialManager, CredentialError

logger = get_logger(__name__)


class ExampleSupplierScraper(BaseScraper):
    """
    Example custom scraper for a supplier with complex requirements.
    
    This demonstrates how to implement a custom scraper when the config-driven
    approach is insufficient for a particular supplier's website.
    """
    
    async def scrape(self) -> ScrapingResult:
        """Perform custom scraping workflow for Example Supplier."""
        import time
        
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            logger.info(f"Starting custom scraping for {self.supplier_name}")
            
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
            logger.error(f"Custom scraping failed for {self.supplier_name}: {str(e)}")
            result.errors.append(f"Scraping error: {str(e)}")
            await self.browser_manager.take_screenshot(f"custom_scraping_error_{self.supplier_name}")
        
        finally:
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    async def authenticate(self) -> bool:
        """Custom authentication logic for Example Supplier."""
        # Example: Multi-step authentication
        login_url = self.config.get('authentication', {}).get('login_url')
        if not login_url:
            logger.error(f"No login_url configured for {self.supplier_name}")
            return False
        
        # Navigate to login page
        if not await self.browser_manager.navigate(login_url):
            return False
        
        # Retrieve and validate credentials using CredentialManager
        auth_cfg = self.config.get('authentication', {})
        try:
            cred_manager = CredentialManager(self.supplier_name, auth_cfg)
            credentials = cred_manager.get_credentials()
            username = credentials.username
            password = credentials.password
        except CredentialError as e:
            logger.error(f"Credential validation failed for {self.supplier_name}: {e}")
            return False
        
        # Custom authentication steps
        # Step 1: Enter username
        page = self.browser_manager._ensure_page_initialized()
        await page.fill('#username', username)
        
        # Step 2: Click "Next" button
        await page.click('#next-button')
        await page.wait_for_load_state('networkidle')
        
        # Step 3: Enter password
        
        await page.fill('#password', password)
        
        # Step 4: Submit form
        await page.click('#login-button')
        await page.wait_for_load_state('networkidle')
        
        # Check if login was successful
        # Look for success indicators or error messages
        try:
            await page.wait_for_selector('.dashboard', timeout=10000)
            logger.info("Authentication successful")
            return True
        except:
            logger.error("Authentication failed - dashboard not found")
            return False
    
    async def navigate_to_downloads(self) -> bool:
        """Custom navigation logic for Example Supplier."""
        # Example: Complex navigation with multiple steps
        
        # Step 1: Click on "Resources" menu
        if not await self.browser_manager.click_and_wait('#resources-menu', 'load'):
            return False
        
        # Step 2: Wait for submenu and click "Price Lists"
        if not await self.browser_manager.wait_for_element('#price-lists-submenu', timeout=5000):
            return False
        
        if not await self.browser_manager.click_and_wait('#price-lists-submenu', 'load'):
            return False
        
        # Step 3: Select brand filter
        brand = self.config.get('brand')
        if brand:
            page = self.browser_manager._ensure_page_initialized()
            await page.select_option('#brand-filter', brand)
            await page.wait_for_load_state('networkidle')
        
        logger.info("Navigation to downloads successful")
        return True
    
    async def download_files(self) -> List[ScrapedFile]:
        """Custom download logic for Example Supplier."""
        files = []
        
        # Example: Download multiple files with custom logic
        
        # Find all download links
        page = self.browser_manager._ensure_page_initialized()
        download_links = await page.query_selector_all('.download-link')
        
        for i, link in enumerate(download_links):
            try:
                # Get file info from link
                filename = await link.get_attribute('data-filename')
                if not filename:
                    filename = f"file_{i+1}.xlsx"
                
                # Click download link
                async with page.expect_download() as download_info:
                    await link.click()
                
                download = await download_info.value
                
                # Save file
                file_path = os.path.join(self.browser_manager.download_dir, filename)
                await download.save_as(file_path)
                
                # Create scraped file
                scraped_file = self.create_scraped_file(
                    filename=filename,
                    local_path=file_path,
                    brand=self.config.get('brand'),
                    location=self.config.get('location'),
                    currency=self.config.get('currency')
                )
                
                files.append(scraped_file)
                logger.info(f"Downloaded file: {filename}")
                
            except Exception as e:
                logger.error(f"Failed to download file {i+1}: {str(e)}")
                continue
        
        return files

