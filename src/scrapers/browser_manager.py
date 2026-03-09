"""
Browser automation manager using Playwright.

Handles browser lifecycle, error handling, screenshots, and provides
a clean interface for website automation tasks.
"""

import os
import tempfile
import random
import asyncio
from pathlib import Path
from typing import Optional, Any, List, Dict
from datetime import datetime

from playwright.async_api import async_playwright, Browser, BrowserContext, Page 
from utils.logger import get_logger

# Type aliases for better type checking
PlaywrightInstance = Any  # async_playwright() return type
LoggerKwargs = Dict[str, Any]  # Type for logger kwargs
logger = get_logger(__name__)


class BrowserManager:
    """
    Manages Playwright browser instances and provides automation utilities.
    
    Handles browser lifecycle, error screenshots, and provides helper methods
    for common automation tasks like form filling and waiting for elements.
    """
    
    def __init__(
        self,
        headless: bool = True,
        download_dir: Optional[str] = None,
        screenshot_dir: Optional[str] = None,
        http_credentials: Optional[Dict[str, str]] = None
    ):
        """
        Initialize browser manager.
        
        Args:
            headless: Whether to run browser in headless mode
            download_dir: Directory for downloaded files (uses temp if None)
            screenshot_dir: Directory for error screenshots (uses temp if None)
            http_credentials: Optional HTTP Basic Auth credentials {'username': 'user', 'password': 'pass'}
        """
        self.headless = headless
        self.download_dir = download_dir or tempfile.mkdtemp(prefix='scraper_downloads_')
        self.screenshot_dir = screenshot_dir or tempfile.mkdtemp(prefix='scraper_screenshots_')
        self.http_credentials = http_credentials
        
        # Ensure directories exist
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)
        Path(self.screenshot_dir).mkdir(parents=True, exist_ok=True)
        
        self.playwright: Optional[PlaywrightInstance] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        logger.debug(
            f"BrowserManager initialized",
            headless=headless,
            download_dir=self.download_dir,
            screenshot_dir=self.screenshot_dir
        )
    
    async def __aenter__(self) -> "BrowserManager":
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
    
    def _ensure_page_initialized(self) -> Page:
        """Ensure page is initialized and return it with proper typing."""
        if self.page is None:
            raise RuntimeError("Browser not started. Call start() first.")
        return self.page

    async def start(self) -> None:
        """Start the browser and create a new context."""
        # Check if already started (idempotent)
        if self.browser is not None and self.page is not None:
            logger.debug("Browser already started, skipping initialization")
            return
        
        import time
        start_time = time.time()
        try:
            logger.info("Starting Playwright instance...")
            playwright_instance = await async_playwright().start()
            self.playwright = playwright_instance
            logger.info("Playwright instance started successfully")
            
            # Use Chromium directly in cloud environment, try Firefox locally (more stable on macOS)
            is_cloud = bool(os.getenv('FUNCTION_TARGET') or os.getenv('K_SERVICE'))
            logger.info(f"Environment: {'Cloud' if is_cloud else 'Local'}")
            
            if not is_cloud:
                try:
                    logger.info("Attempting to launch Firefox browser...")
                    # Launch Firefox with args to look more like a real browser
                    firefox_args = []
                    if not self.headless:
                        firefox_args = ['-width=1920', '-height=1080']
                        
                    self.browser = await playwright_instance.firefox.launch(
                        headless=self.headless,
                        firefox_user_prefs={
                            # Disable automation flags
                            'dom.webdriver.enabled': False,
                            'useAutomationExtension': False,
                            # Make it look more real
                            'general.useragent.override': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/115.0',
                            # Disable tracking protection (sometimes blocks login forms)
                            'privacy.trackingprotection.enabled': False,
                        },
                        args=firefox_args
                    )
                    logger.info(f"Firefox browser launched successfully (headless={self.headless})")
                except Exception as firefox_error:
                    logger.warning(f"Firefox launch failed: {firefox_error}, falling back to Chromium")
                    self.browser = await playwright_instance.chromium.launch(
                        headless=self.headless,
                        args=[
                            '--no-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-gpu',
                            # REMOVED: '--disable-web-security' - Breaks cookies and CSRF!
                            # REMOVED: '--allow-running-insecure-content' - Not needed
                        ]
                    )
            else:
                # Cloud environment - use Chromium directly (no Firefox attempt)
                logger.info("Cloud environment detected, launching Chromium browser...")
                self.browser = await playwright_instance.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--no-sandbox',              # Required for Cloud Run (no user namespaces)
                        '--disable-dev-shm-usage',   # Use /tmp instead of /dev/shm (limited in containers)
                        '--disable-gpu',             # No GPU in Cloud Run containers
                        # REMOVED: '--disable-web-security' - This breaks cookies and CSRF!
                        # REMOVED: '--allow-running-insecure-content' - Not needed, causes issues
                    ]
                )
                logger.info(f"Chromium browser launched successfully with secure cookie handling (headless={self.headless})")
            
            # Create context with download enabled (downloads will be saved via save_as)
            # Set Dutch locale for Netherlands sites
            context_options: Dict[str, Any] = {
                'accept_downloads': True,
                'locale': 'nl-NL',  # Dutch (Netherlands)
                'timezone_id': 'Europe/Amsterdam',
                'extra_http_headers': {
                    'Accept-Language': 'nl-NL,nl;q=0.9,en;q=0.8'
                }
            }
            
            # Add HTTP Basic Auth if credentials provided
            if self.http_credentials:
                context_options['http_credentials'] = self.http_credentials
                logger.info("HTTP Basic Auth credentials configured")
            
            logger.info("Creating browser context...")
            self.context = await self.browser.new_context(**context_options)
            logger.info("Browser context created successfully")
            
            # Create page
            logger.info("Creating new page...")
            self.page = await self.context.new_page()
            
            # Set longer timeout for Cloud Run environment (is_cloud already set above)
            base_timeout = 60000 if is_cloud else 30000
            self.page.set_default_timeout(base_timeout)
            logger.info(f"Page created successfully (default timeout: {base_timeout/1000}s)")
            
            total_startup_time = time.time() - start_time
            logger.info(f"Browser started successfully (total startup time: {total_startup_time:.2f}s)")
            
        except Exception as e:
            logger.error(f"Failed to start browser: {str(e)}")
            await self.close()
            raise
    
    async def close(self) -> None:
        """Close browser and cleanup resources."""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.error(f"Error closing browser: {str(e)}")
        finally:
            self.page = None
            self.context = None
            self.browser = None
            self.playwright = None
            
        logger.debug("Browser closed successfully")
    
    async def navigate(self, url: str) -> bool:
        """
        Navigate to a URL.
        
        Args:
            url: URL to navigate to
            
        Returns:
            True if navigation successful, False otherwise
        """
        try:
            logger.info(f"Navigating to: {url}")
            page = self._ensure_page_initialized()
            # Increased timeout to 60s for Cloud Run (default 30s is too short)
            import time
            nav_start = time.time()
            logger.info(f"Starting page.goto() with 60s timeout...")
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            nav_time = time.time() - nav_start
            
            # Log success with timing and page title
            try:
                page_title = await page.title()
                logger.info(f"Navigation completed in {nav_time:.2f}s - Title: '{page_title}' - URL: {page.url}")
            except Exception:
                logger.info(f"Navigation completed in {nav_time:.2f}s - URL: {page.url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to navigate to {url}: {str(e)}")
            await self.take_screenshot(f"navigation_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            return False
    
    async def fill_form(
        self,
        username_field: str,
        password_field: str,
        username: Optional[str],
        password: Optional[str],
        submit_button: Optional[str] = None
    ) -> bool:
        """
        Fill login form fields with human-like behavior.
        
        Uses realistic delays, mouse movements, and typing speed to avoid bot detection.
        
        Args:
            username_field: CSS selector for username field
            password_field: CSS selector for password field
            username: Username to enter
            password: Password to enter
            submit_button: CSS selector for submit button (optional)
            
        Returns:
            True if form filled successfully, False otherwise
        """
        try:
            logger.info(f"Filling form fields with human-like behavior")
            page = self._ensure_page_initialized()
            
            # Random initial pause (human reads the page first)
            await asyncio.sleep(1.0)
            
            # Fill username - robust approach
            if username:
                try:
                    logger.info("Filling username field...")
                    await page.fill(username_field, username)
                    await asyncio.sleep(0.5)
                    logger.info("Username filled successfully")
                except Exception as e:
                    logger.error(f"Error filling username: {e}")
                    raise
            
            # Fill password - robust approach
            if password:
                try:
                    logger.info("Filling password field...")
                    await page.fill(password_field, password)
                    await asyncio.sleep(0.5)
                    logger.info("Password filled successfully")
                except Exception as e:
                    logger.error(f"Error filling password: {e}")
                    raise
            
            # Final pause before submit
            await asyncio.sleep(1.0)
            
            # Click submit button if provided
            if submit_button:
                # Skip mouse movements for now - may trigger detection
                # try:
                #     # Move mouse to submit button
                #     submit_element = await page.query_selector(submit_button)
                #     if submit_element:
                #         box = await submit_element.bounding_box()
                #         if box and box['width'] > 20 and box['height'] > 10:
                #             x = box['x'] + random.uniform(10, min(box['width'] - 10, box['width'] * 0.8))
                #             y = box['y'] + box['height'] / 2
                #             await page.mouse.move(x, y)
                #             await asyncio.sleep(random.uniform(0.2, 0.4))
                # except Exception as mouse_err:
                #     logger.warning(f"Mouse movement to submit failed (non-critical): {mouse_err}")
                
                # Click the button
                logger.info(f"Clicking submit button: {submit_button}")
                
                # Use Promise.all to wait for navigation triggered by form submit
                # This ensures we catch the actual form POST, not just page reload
                try:
                    async with page.expect_navigation(timeout=60000, wait_until='networkidle'):
                        await page.click(submit_button)
                    logger.info("Form submitted and navigation completed")
                except Exception as nav_error:
                    logger.warning(f"Navigation after submit didn't trigger (may be AJAX): {nav_error}")
                    # If no navigation, wait for network to settle
                    await page.wait_for_load_state('networkidle', timeout=60000)
            
            logger.info("Form filled successfully with human-like behavior")
            return True
            
        except Exception as e:
            logger.error(f"Failed to fill form: {str(e)}")
            await self.take_screenshot(f"form_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            return False
    
    async def click_and_wait(
        self,
        selector: str,
        wait_for: str = 'load',
        timeout: int = 30000
    ) -> bool:
        """
        Click an element and wait for page state.
        
        Args:
            selector: CSS selector for element to click
            wait_for: What to wait for ('load', 'networkidle', 'domcontentloaded')
            timeout: Timeout in milliseconds
            
        Returns:
            True if click and wait successful, False otherwise
        """
        try:
            logger.info(f"Clicking element: {selector}")
            page = self._ensure_page_initialized()
            
            await page.click(selector)
            
            if wait_for == 'load':
                await page.wait_for_load_state('load', timeout=timeout)
            elif wait_for == 'networkidle':
                await page.wait_for_load_state('networkidle', timeout=timeout)
            elif wait_for == 'domcontentloaded':
                await page.wait_for_load_state('domcontentloaded', timeout=timeout)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to click {selector}: {str(e)}")
            await self.take_screenshot(f"click_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            return False
    
    async def wait_for_element(
        self,
        selector: str,
        timeout: int = 30000
    ) -> bool:
        """
        Wait for an element to appear on the page.
        
        Args:
            selector: CSS selector for element to wait for
            timeout: Timeout in milliseconds
            
        Returns:
            True if element found, False if timeout
        """
        try:
            page = self._ensure_page_initialized()
            await page.wait_for_selector(selector, timeout=timeout)
            return True
            
        except Exception:
            logger.error(f"Element not found: {selector}")
            await self.take_screenshot(f"element_wait_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            return False
    
    async def download_file(self, download_selector: str) -> Optional[str]:
        """
        Trigger a file download and return the local file path.
        
        Args:
            download_selector: CSS selector for download link/button
            
        Returns:
            Local file path if download successful, None otherwise
        """
        try:
            logger.info(f"Starting download via: {download_selector}")
            page = self._ensure_page_initialized()
            
            # Set up download promise
            async with page.expect_download() as download_info:
                await page.click(download_selector)
            
            download = await download_info.value
            
            # Generate filename if needed
            suggested_filename = download.suggested_filename
            if not suggested_filename:
                suggested_filename = f"downloaded_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Save file
            file_path = os.path.join(self.download_dir, suggested_filename)
            await download.save_as(file_path)
            
            logger.info(f"File downloaded: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            await self.take_screenshot(f"download_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            return None
    
    async def get_page_text(self, selector: Optional[str] = None) -> str:
        """
        Get text content from page or specific element.
        
        Args:
            selector: CSS selector for specific element (optional)
            
        Returns:
            Text content
        """
        try:
            page = self._ensure_page_initialized()
            if selector:
                element = await page.query_selector(selector)
                if element:
                    return await element.text_content() or ""
                return ""
            else:
                return await page.text_content('body') or ""
                
        except Exception as e:
            logger.error(f"Failed to get text content: {str(e)}")
            return ""
    
    async def take_screenshot(self, name: str) -> str:
        """
        Take a screenshot for debugging (if enabled).
        
        Args:
            name: Base name for screenshot file
            
        Returns:
            Path to screenshot file (empty string if disabled or failed)
        """
        # Check if screenshots are enabled (default: False for performance)
        # Special case: Always take screenshots for FAILED/TIMEOUT events
        enable_screenshots = os.getenv('ENABLE_SCREENSHOTS', 'false').lower() == 'true'
        is_error_screenshot = 'FAILED' in name.upper() or 'TIMEOUT' in name.upper() or 'ERROR' in name.upper()
        
        if not enable_screenshots and not is_error_screenshot:
            logger.debug(f"Screenshot '{name}' skipped (ENABLE_SCREENSHOTS not set)")
            return ""
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{name}_{timestamp}.png"
            file_path = os.path.join(self.screenshot_dir, filename)
            
            page = self._ensure_page_initialized()
            await page.screenshot(path=file_path, full_page=True)
            
            logger.info(f"Screenshot saved locally: {file_path}")
            
            # Upload to GCS if in cloud environment (for debugging)
            if os.getenv('K_SERVICE') or os.getenv('FUNCTION_TARGET'):
                try:
                    from google.cloud import storage
                    bucket_name = os.getenv('GCS_BUCKET')
                    if bucket_name:
                        storage_client = storage.Client()
                        bucket = storage_client.bucket(bucket_name)
                        blob_path = f"screenshots/{filename}"
                        blob = bucket.blob(blob_path)
                        blob.upload_from_filename(file_path)
                        logger.info(f"Screenshot uploaded to GCS: gs://{bucket_name}/{blob_path}")
                    else:
                        logger.warning("GCS_BUCKET not set, screenshot not uploaded")
                except Exception as upload_error:
                    logger.warning(f"Failed to upload screenshot to GCS: {upload_error}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to take screenshot: {str(e)}")
            return ""
    
    def get_downloaded_files(self) -> List[str]:
        """
        Get list of files in download directory.
        
        Returns:
            List of file paths
        """
        try:
            download_path = Path(self.download_dir)
            return [str(f) for f in download_path.iterdir() if f.is_file()]
        except Exception as e:
            logger.error(f"Failed to list downloaded files: {str(e)}")
            return []
