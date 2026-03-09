"""
Brechmann-specific scraper.

Brechmann uses a dropdown to select brands and an export button that triggers
email delivery rather than direct downloads.
"""

from typing import List, Dict, Any, Optional
import re
import asyncio
import time
from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import find_matching_brand, load_brand_configs
from utils.logger import get_logger
from utils.credential_manager import CredentialManager, CredentialError

logger = get_logger(__name__)


class BrechmannScraper(BaseScraper):
    """Custom scraper for Brechmann price lists."""
    
    price_list_url: str  # Set during navigate_to_downloads()
    
    async def scrape(self) -> ScrapingResult:
        """
        Scrape Brechmann price lists by:
        1. Logging in
        2. Navigating to price list page
        3. Extracting available brands from dropdown
        4. Filtering by brand_config.json
        5. Triggering email export for each brand
        """
        import time
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            await self.browser_manager.start()
            
            # Step 1: Authenticate
            try:
                auth_ok = await self.authenticate()
                if not auth_ok:
                    result.errors.append('Authentication failed')
                    return result
            except Exception as auth_error:
                error_msg = f'Authentication failed: {str(auth_error)}'
                result.errors.append(error_msg)
                logger.error(error_msg)
                return result
            
            # Step 2: Navigate to price list page
            nav_ok = await self.navigate_to_downloads()
            if not nav_ok:
                result.errors.append('Navigation to price list page failed')
                return result
            
            # Step 3: Extract and filter brands
            brands_to_export = await self._get_filtered_brands()
            
            # Track file counts for reporting (BRECHMANN triggers email exports, not direct downloads)
            self.total_files_found = len(brands_to_export)
            self.files_skipped_duplicates = 0
            
            if not brands_to_export:
                logger.warning("No brands matched after filtering")
                result.success = True  # Not an error, just no matches
                return result
            
            logger.info(f"Found {len(brands_to_export)} brands to export")
            
            # Step 4: Trigger export for each brand
            exported_count = 0
            failed_brands: List[str] = []
            
            for idx, brand_info in enumerate(brands_to_export, 1):
                logger.info(f"Processing brand {idx}/{len(brands_to_export)}: {brand_info['name']}")
                
                success = await self._trigger_export(brand_info)
                if success:
                    exported_count += 1
                else:
                    failed_brands.append(brand_info['name'])
                
                # Navigate back to price list page for next export (clean state)
                if idx < len(brands_to_export):
                    logger.info(f"Returning to price list page for next brand...")
                    await self.browser_manager.navigate(self.price_list_url)
                    await asyncio.sleep(2)  # 2 second delay before next export
            
            logger.info(f"Export summary: {exported_count}/{len(brands_to_export)} successful")
            if failed_brands:
                logger.warning(f"Failed exports for brands: {', '.join(failed_brands)}")
            logger.info(f"Export method: email - files will be delivered via email to be picked up by email processor")
            
            result.success = exported_count > 0
            # Note: files list is empty because exports go via email, not direct download
            
        except Exception as e:
            logger.error(f"Brechmann scraping failed: {str(e)}")
            result.errors.append(str(e))
        finally:
            await self.browser_manager.close()
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    async def authenticate(self) -> bool:
        """Authenticate to Brechmann."""
        auth_cfg = self.config.get('authentication', {})
        login_url = auth_cfg.get('login_url')
        
        if not login_url:
            return True
        
        try:
            # Navigate to login page
            await self.browser_manager.navigate(login_url)
            page = self.browser_manager._ensure_page_initialized()
            logger.info(f"✓ Navigation to login page completed")
            logger.info(f"Page loaded, current URL: {page.url}")
            await self.browser_manager.take_screenshot("00_before_login")
            
            # Dismiss CCM19 cookie consent modal (blocks login button if not dismissed)
            page = self.browser_manager._ensure_page_initialized()
            try:
                # Wait for modal to potentially appear after page load
                await page.wait_for_timeout(2000)
                
                # Check for CCM19 modal (uses #ccm-widget or .ccm-modal)
                ccm_modal = await page.query_selector('#ccm-widget, .ccm-modal')
                if ccm_modal and await ccm_modal.is_visible():
                    logger.info("CCM19 cookie consent modal detected")
                    
                    # Try multiple selectors for "ACCEPT ALL" button
                    accept_selectors = [
                        'button:has-text("ACCEPT ALL")',
                        'button:has-text("Accept all")',
                        '#ccm-widget button:last-of-type',
                    ]
                    
                    dismissed = False
                    for selector in accept_selectors:
                        try:
                            accept_btn = await page.query_selector(selector)
                            if accept_btn and await accept_btn.is_visible():
                                await accept_btn.click()
                                logger.info(f"Dismissed CCM19 modal using: {selector}")
                                await page.wait_for_timeout(1500)
                                dismissed = True
                                break
                        except Exception:
                            continue
                    
                    if not dismissed:
                        logger.warning("Could not find CCM19 accept button - login may fail")
                else:
                    logger.info("No CCM19 cookie modal detected")
            except Exception as e:
                logger.warning(f"Error handling CCM19 modal: {e}")
            
            # Retrieve and validate credentials using CredentialManager
            try:
                cred_manager = CredentialManager(self.supplier_name, auth_cfg)
                credentials = cred_manager.get_credentials()
                username = credentials.username
                password = credentials.password
                
                # DEBUG: Log credentials (obfuscated password)
                password_obfuscated = f"{password[0]}***{password[-1]}" if len(password) > 2 else "***"
                logger.info(f"DEBUG: Using credentials - username: {username}, password: {password_obfuscated} (length: {len(password)})")
            except CredentialError as e:
                logger.error(f"Credential validation failed for {self.supplier_name}: {e}")
                raise
            
            # Fill and submit form
            username_field = auth_cfg.get('username_field', 'input[name="username"]')
            password_field = auth_cfg.get('password_field', 'input[name="password"]')
            submit_button = auth_cfg.get('submit_button', 'button[type="submit"]')
            
            logger.info(f"Filling form fields: username_field={username_field}, password_field={password_field}")
            await page.fill(username_field, username)
            await page.wait_for_timeout(500)
            await page.fill(password_field, password)
            await page.wait_for_timeout(500)
            logger.info("Form fields filled successfully")
            
            await self.browser_manager.take_screenshot("00_before_submit")
            
            logger.info(f"Submitting login form using button: {submit_button}")
            start_time = time.time()
            await page.click(submit_button, timeout=60000)
            logger.info(f"Login button clicked at {time.time() - start_time:.1f}s, waiting for networkidle (120s timeout)...")
            
            try:
                # Increased timeout to 120s for cloud environment
                await page.wait_for_load_state('networkidle', timeout=120000)
                elapsed = time.time() - start_time
                logger.info(f"Login completed (networkidle) after {elapsed:.1f}s")
            except Exception as e:
                elapsed = time.time() - start_time
                logger.warning(f"networkidle timeout after {elapsed:.1f}s: {e}")
                # Check if we're still on login page or actually logged in
                current_url = page.url
                current_title = await page.title()
                logger.info(f"After timeout - URL: {current_url}, Title: {current_title}")
                
                # If we're no longer on login page, consider it successful
                if 'login' not in current_url.lower():
                    logger.info(f"Login appears successful despite networkidle timeout (redirected to: {current_url})")
                    # Continue instead of raising
                else:
                    logger.error(f"Still on login page after {elapsed:.1f}s - authentication likely failed")
                    raise
            
            await self.browser_manager.take_screenshot("00_after_login")
            
            # Check if login was successful
            current_url = page.url
            if 'login' not in current_url.lower():
                logger.info(f"Login successful, redirected to: {current_url}")
                return True
            else:
                error_msg = "Still on login page after submit - credentials may be incorrect"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            # Don't log here - let scrape() handle logging to avoid duplicates
            raise  # Re-raise to allow scrape() to capture the detailed error
    
    async def navigate_to_downloads(self) -> bool:
        """Navigate to the price list page."""
        try:
            self.price_list_url = self.config.get('links', {}).get('page_url', 'https://www.brechmann.parts/dashboard/price-list')
            await self.browser_manager.navigate(self.price_list_url)
            await self.browser_manager.take_screenshot("01_pricelist_page")
            return True
        except Exception as e:
            logger.error(f"Failed to navigate to price list page: {str(e)}")
            return False
    
    async def download_files(self) -> List[ScrapedFile]:
        """Not used for Brechmann (exports via email)."""
        return []
    
    async def _get_filtered_brands(self) -> List[Dict[str, Any]]:
        """
        Extract available brands from dropdown and filter against brand_config.json.
        
        Returns:
            List of dicts with 'name' and 'value' (option value for export URL)
        """
        page = self.browser_manager._ensure_page_initialized()
        
        try:
            # Extract all options from the Choices.js dropdown (custom div-based dropdown)
            brands_data = await page.evaluate("""
                () => {
                    // Brechmann uses Choices.js which replaces <select> with divs
                    // Look for enabled items (not disabled ones)
                    const items = document.querySelectorAll('.choices__item--choice:not(.choices__item--disabled)');
                    const brands = [];
                    
                    items.forEach(item => {
                        const value = item.getAttribute('data-value');
                        const name = item.textContent.trim();
                        // Only include items with actual values (not empty)
                        if (value && name && value !== '') {
                            brands.push({name: name, value: value});
                        }
                    });
                    
                    return brands;
                }
            """)
            
            logger.info(f"Found {len(brands_data)} brands in dropdown: {[b['name'] for b in brands_data]}")
            
            # Load brand configs for filtering
            brand_configs = load_brand_configs()
            if not brand_configs:
                logger.error("Failed to load brand configs")
                return []
            
            # Filter brands
            filtered_brands: List[Dict[str, Any]] = []
            matched_brands: Dict[str, int] = {}
            skipped_brands: Dict[str, int] = {}
            
            for brand_data in brands_data:
                brand_name = brand_data['name']
                
                # Try to match against brand configs
                matched_config = find_matching_brand(brand_name, brand_configs)
                
                if matched_config:
                    brand_data['_matched_brand_config'] = matched_config
                    filtered_brands.append(brand_data)
                    
                    config_brand_name = matched_config.get('brand', brand_name)
                    matched_brands[config_brand_name] = matched_brands.get(config_brand_name, 0) + 1
                else:
                    skipped_brands[brand_name] = skipped_brands.get(brand_name, 0) + 1
            
            # Log filtering results
            logger.info(f"Brand filtering: {len(filtered_brands)}/{len(brands_data)} brands matched")
            
            if matched_brands:
                matched_summary = ", ".join(sorted(matched_brands.keys()))
                logger.info(f"Matched brands: {matched_summary}")
            
            if skipped_brands:
                skipped_summary = ", ".join(sorted(skipped_brands.keys()))
                logger.warning(f"Skipped brands (not in config): {skipped_summary}")
            
            return filtered_brands
            
        except Exception as e:
            logger.error(f"Failed to extract brands from dropdown: {str(e)}")
            return []
    
    async def _trigger_export(self, brand_info: Dict[str, Any]) -> bool:
        """
        Trigger email export for a specific brand.
        
        Args:
            brand_info: Dict with 'name', 'value', and '_matched_brand_config'
            
        Returns:
            True if export was triggered successfully
        """
        page = self.browser_manager._ensure_page_initialized()
        brand_name = brand_info['name']
        brand_value = brand_info['value']
        
        try:
            logger.info(f"Triggering export for brand: {brand_name}")
            
            # Construct export URL
            export_url = f"https://www.brechmann.parts/dashboard/price-list/export?productManufacturer={brand_value}&search_terms="
            logger.info(f"Export URL: {export_url}")
            
            # Navigate to export URL (triggers the export)
            logger.info(f"Navigating to export URL for {brand_name}...")
            start_time = time.time()
            await page.goto(export_url, wait_until='domcontentloaded', timeout=90000)
            elapsed = time.time() - start_time
            logger.info(f"Export page loaded after {elapsed:.1f}s")
            logger.info(f"Navigation complete for {brand_name}")
            
            # Wait a bit for any dynamic content
            await asyncio.sleep(1)
            
            # Take screenshot after export attempt
            screenshot_name = f"02_export_{brand_name.lower().replace(' ', '_')}"
            await self.browser_manager.take_screenshot(screenshot_name)
            
            # Check multiple success indicators
            current_url = page.url
            logger.info(f"Current URL: {current_url}")
            
            # Get page title and visible text
            page_title = await page.title()
            logger.info(f"Page title: {page_title}")
            
            # Check for success indicators
            success_indicators = [
                'export-success' in current_url.lower(),
                'success' in page_title.lower(),
                'erfolg' in page_title.lower(),  # German for success
            ]
            
            # Check for error indicators
            error_indicators_present = False
            try:
                error_text = await page.text_content('body')
                if error_text and any(word in error_text.lower() for word in ['error', 'fehler', 'failed']):
                    logger.warning(f"Error text found on page for {brand_name}")
                    error_indicators_present = True
            except Exception:
                pass
            
            # Determine success
            is_success = any(success_indicators) and not error_indicators_present
            
            if is_success:
                logger.info(f"✓ Export triggered successfully for {brand_name}")
                return True
            else:
                logger.warning(f"✗ Export for {brand_name} may have failed")
                logger.warning(f"  - URL contains 'export-success': {success_indicators[0]}")
                logger.warning(f"  - Title contains 'success': {success_indicators[1]}")
                logger.warning(f"  - Error text found: {error_indicators_present}")
                return False
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            
            if 'timeout' in error_msg.lower():
                logger.error(f"⏱ Timeout waiting for {brand_name} export page to load (90s)")
                logger.error(f"   This may indicate: very large file, server issues, or page stuck loading")
            else:
                logger.error(f"Failed to trigger export for {brand_name}: {error_type} - {error_msg}")
            
            # Take screenshot on error
            try:
                screenshot_name = f"02_export_error_{brand_name.lower().replace(' ', '_')}"
                await self.browser_manager.take_screenshot(screenshot_name)
                logger.info(f"Error screenshot saved: {screenshot_name}")
            except Exception:
                pass
            return False

