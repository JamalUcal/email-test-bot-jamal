"""
Link Downloader Scraper template.

Logs in via form, navigates to a page and downloads files by clicking anchor links
that match a configured selector or href pattern.

Used for TECHNOPARTS and CONNEX.
"""

from typing import List, Dict, Any, Optional, AsyncIterator
import os
import re
import json
from urllib.parse import urlparse, parse_qs
from datetime import datetime

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import extract_brand_from_text, normalize_brand, find_matching_brand, load_brand_configs
from scrapers.version_detector import VersionDetector
from utils.logger import get_logger
from utils.credential_manager import CredentialManager, CredentialError

logger = get_logger(__name__)


class LinkDownloaderScraper(BaseScraper):
    def __init__(self, browser_manager: Any, scraper_config: Dict[str, Any], core_config: Dict[str, Any], service_account_info: Dict[str, Any], start_index: int = 0, state_manager: Optional[Any] = None):
        super().__init__(scraper_config, browser_manager, start_index, state_manager)
        self.core_config = core_config
        self.service_account_info = service_account_info
        self.version_detector = VersionDetector()
        # Track file counts for duplicate detection reporting
        self.total_files_found: int = 0
        self.files_skipped_duplicates: int = 0
    
    async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
        """
        Stream scraped files one at a time for immediate processing.
        
        This method downloads and yields files one-by-one, allowing the orchestrator
        to process each file (parse, convert, upload) before moving to the next download.
        
        Yields:
            ScrapedFile objects as they are downloaded
        """
        try:
            await self.browser_manager.start()
            auth_ok = await self._authenticate()
            if not auth_ok:
                logger.error('Authentication failed')
                return
            
            links_cfg = self.config.get('links', {})
            page_url = links_cfg['page_url']
            await self.browser_manager.navigate(page_url)
            
            # Take screenshot for debugging
            await self.browser_manager.take_screenshot("01_pricelist_page")
            
            link_selector = links_cfg.get('link_selector', 'a')
            href_pattern = links_cfg.get('link_href_pattern')
            
            # Find links and collect with hrefs
            page = self.browser_manager._ensure_page_initialized()
            
            # Debug: Log page info
            page_title = await page.title()
            page_url_actual = page.url
            logger.info(f"Page loaded: title='{page_title}', url={page_url_actual}")
            
            # Debug: Log all links on the page
            all_links = await page.query_selector_all('a')
            logger.info(f"Total links on page: {len(all_links)}")
            
            # If no links found, dump page content for debugging
            if len(all_links) == 0:
                page_content = await page.content()
                logger.warning(f"No links found on page. Page content preview (first 500 chars):")
                logger.warning(page_content[:500])
                # Also check if there are any table rows (directory listings often use tables)
                table_rows = await page.query_selector_all('tr')
                logger.info(f"Found {len(table_rows)} table rows on page")
                if table_rows:
                    logger.info("Page appears to be a table-based directory listing")
            
            for idx, link in enumerate(all_links[:5]):  # Show first 5
                href = await link.get_attribute('href')
                text = await link.inner_text()
                logger.info(f"Sample link {idx+1}: href={href}, text={text[:50] if text else 'N/A'}")
            
            elements = await page.query_selector_all(link_selector)
            
            # Collect links with their hrefs for brand filtering
            link_items: List[Dict[str, Any]] = []
            for el in elements:
                href = await el.get_attribute('href')
                if not href:
                    continue
                if href_pattern:
                    # Use regex match for more precise matching
                    if not re.search(href_pattern, href):
                        continue
                
                link_items.append({
                    'element': el,
                    'href': href
                })
            
            logger.info(f"Found {len(link_items)} links matching pattern")
            
            # Extract directory listing metadata (dates) if present
            page_content = await page.content()
            directory_metadata = self._extract_directory_listing_metadata(page_content)
            
            # Add metadata to link items
            for link_item in link_items:
                filename = os.path.basename(urlparse(link_item['href']).path)
                if filename in directory_metadata:
                    link_item['modified'] = directory_metadata[filename].get('modified')
                    logger.debug(f"Found metadata for {filename}: modified={link_item.get('modified')}")
            
            # Filter links by brand BEFORE downloading
            filtered_links = await self._filter_links_by_brand(link_items)
            
            # Track total files found after brand filtering (for duplicate detection reporting)
            self.total_files_found = len(filtered_links)
            
            # Apply max_files limit if configured
            limits = self.config.get('limits', {})
            max_files = limits.get('max_files', None)
            if max_files is not None:
                logger.info(f"Limiting to {max_files} files (total available: {len(filtered_links)})")
                filtered_links = filtered_links[:max_files]
            
            # Download and yield files one at a time
            files_downloaded = 0
            for idx, link_item in enumerate(filtered_links, 1):
                # Skip already processed files if resuming from interruption
                if idx <= self.start_index:
                    logger.info(f"[RESUME] Skipping file {idx}/{len(filtered_links)} (resuming from index {self.start_index})")
                    continue
                
                el = link_item['element']
                href = link_item['href']
                
                # Extract brand for duplicate checking
                matched_brand_config = link_item.get('_matched_brand_config')
                brand = matched_brand_config.get('brand') if matched_brand_config else link_item.get('_extracted_brand')
                
                # Extract supplier's filename from href for duplicate detection
                supplier_filename = os.path.basename(urlparse(href).path)
                valid_from_date_str = None
                
                if self.state_manager:
                    # Try to detect version/date from filename or metadata
                    detection_mode = self.config.get('schedule', {}).get('detection_mode', 'date_based')
                    if detection_mode == 'date_based':
                        # Build item dict with filename, href, and any extracted metadata
                        item_dict = {'filename': supplier_filename, 'href': href}
                        if 'modified' in link_item:
                            item_dict['modified'] = link_item['modified']
                            logger.info(f"[METADATA] Found 'modified' field in link_item: {link_item['modified']}", filename=supplier_filename)
                        else:
                            logger.warning(f"[METADATA] No 'modified' field in link_item for {supplier_filename}", link_item_keys=list(link_item.keys()))
                        
                        logger.info(f"[VERSION DETECTION] Calling version_detector with item_dict: {item_dict}", filename=supplier_filename)
                        version = self.version_detector.detect_version(
                            item=item_dict,
                            detection_mode=detection_mode
                        )
                        if version:
                            valid_from_date_str = version
                            logger.info(f"[VERSION DETECTED] Extracted date: {version}", filename=supplier_filename)
                        else:
                            logger.warning(f"[VERSION DETECTION FAILED] No version detected from item_dict: {item_dict}", filename=supplier_filename)
                    
                    logger.info(
                        f"[DUPLICATE CHECK] File {idx}/{len(filtered_links)}: supplier_filename={supplier_filename}, valid_from={valid_from_date_str}",
                        supplier_filename=supplier_filename,
                        valid_from_date=valid_from_date_str
                    )
                    
                    # Check if already processed (using supplier filename + date)
                    if supplier_filename and valid_from_date_str:
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename,
                            valid_from_date=valid_from_date_str
                        ):
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(filtered_links)} - Already have: {supplier_filename} valid from {valid_from_date_str}. NOT downloading.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                            self.files_skipped_duplicates += 1
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(filtered_links)} - Not in state: {supplier_filename} valid from {valid_from_date_str}. Will download.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                    elif supplier_filename:
                        # Fallback: check by filename if no date
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename
                        ):
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(filtered_links)} - Already have: {supplier_filename}. NOT downloading.",
                                supplier_filename=supplier_filename
                            )
                            self.files_skipped_duplicates += 1
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(filtered_links)} - Not in state: {supplier_filename}. Will download.",
                                supplier_filename=supplier_filename
                            )
                
                files_downloaded += 1
                logger.info(f"[DOWNLOAD] Downloading file {idx}/{len(filtered_links)} (file #{files_downloaded})")
                
                # Download
                file_path = await self._download_via_click(el)
                if not file_path:
                    continue
                
                # Generate normalized filename
                original_filename = os.path.basename(file_path)
                matched_brand_config = link_item.get('_matched_brand_config')
                raw_supplier_brand = matched_brand_config.get('brand') if matched_brand_config else link_item.get('_extracted_brand')
                
                normalized_filename = self._derive_filename(href, original_filename, raw_supplier_brand, valid_from_date_str)
                
                # Rename file to normalized name
                normalized_path = os.path.join(os.path.dirname(file_path), normalized_filename)
                if normalized_path != file_path:
                    os.replace(file_path, normalized_path)
                    file_path = normalized_path
                
                # Create scraped file and yield immediately for processing
                # Pass supplier_filename and valid_from_date_str for duplicate detection
                scraped_file = self.create_scraped_file(
                    normalized_filename, 
                    file_path, 
                    brand=raw_supplier_brand,
                    supplier_filename=supplier_filename,  # Original filename from website for duplicate detection
                    valid_from_date_str=valid_from_date_str  # Date extracted from directory listing
                )
                logger.info(f"Successfully downloaded file: {normalized_filename} (valid_from: {valid_from_date_str})")
                yield scraped_file
        
        except Exception as e:
            logger.error(f"Link downloader scraping failed: {str(e)}")
            raise
        
        finally:
            await self.browser_manager.close()
    
    async def scrape(self) -> ScrapingResult:
        import time
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            await self.browser_manager.start()
            auth_ok = await self._authenticate()
            if not auth_ok:
                result.errors.append('Authentication failed')
                return result
            
            links_cfg = self.config.get('links', {})
            page_url = links_cfg['page_url']
            await self.browser_manager.navigate(page_url)
            
            # Take screenshot for debugging
            await self.browser_manager.take_screenshot("01_pricelist_page")
            
            link_selector = links_cfg.get('link_selector', 'a')
            href_pattern = links_cfg.get('link_href_pattern')
            
            # Find links and collect with hrefs
            page = self.browser_manager._ensure_page_initialized()
            
            # Debug: Log page info
            page_title = await page.title()
            page_url_actual = page.url
            logger.info(f"Page loaded: title='{page_title}', url={page_url_actual}")
            
            # Debug: Log all links on the page
            all_links = await page.query_selector_all('a')
            logger.info(f"Total links on page: {len(all_links)}")
            
            # If no links found, dump page content for debugging
            if len(all_links) == 0:
                page_content = await page.content()
                logger.warning(f"No links found on page. Page content preview (first 500 chars):")
                logger.warning(page_content[:500])
                # Also check if there are any table rows (directory listings often use tables)
                table_rows = await page.query_selector_all('tr')
                logger.info(f"Found {len(table_rows)} table rows on page")
                if table_rows:
                    logger.info("Page appears to be a table-based directory listing")
            
            for idx, link in enumerate(all_links[:5]):  # Show first 5
                href = await link.get_attribute('href')
                text = await link.inner_text()
                logger.info(f"Sample link {idx+1}: href={href}, text={text[:50] if text else 'N/A'}")
            
            elements = await page.query_selector_all(link_selector)
            
            # Collect links with their hrefs for brand filtering
            link_items: List[Dict[str, Any]] = []
            for el in elements:
                href = await el.get_attribute('href')
                if not href:
                    continue
                if href_pattern:
                    # Use regex match for more precise matching
                    import re
                    if not re.search(href_pattern, href):
                        continue
                
                link_items.append({
                    'element': el,
                    'href': href
                })
            
            logger.info(f"Found {len(link_items)} links matching pattern")
            
            # Filter links by brand BEFORE downloading
            filtered_links = await self._filter_links_by_brand(link_items)
            
            # Track total files found after brand filtering (for duplicate detection reporting)
            self.total_files_found = len(filtered_links)
            
            # Apply max_files limit if configured
            limits = self.config.get('limits', {})
            max_files = limits.get('max_files', None)
            if max_files is not None:
                logger.info(f"Limiting to {max_files} files (total available: {len(filtered_links)})")
                filtered_links = filtered_links[:max_files]
            
            # Download filtered files
            files: List[ScrapedFile] = []
            files_downloaded = 0
            for idx, link_item in enumerate(filtered_links, 1):
                # Skip already processed files if resuming from interruption
                if idx <= self.start_index:
                    logger.info(f"[RESUME] Skipping file {idx}/{len(filtered_links)} (resuming from index {self.start_index})")
                    continue
                
                el = link_item['element']
                href = link_item['href']
                
                # Extract brand for duplicate checking
                matched_brand_config = link_item.get('_matched_brand_config')
                brand = matched_brand_config.get('brand') if matched_brand_config else link_item.get('_extracted_brand')
                
                # Extract supplier's filename from href for duplicate detection
                supplier_filename = os.path.basename(urlparse(href).path)
                valid_from_date_str = None
                
                if self.state_manager:
                    # Try to detect version/date from filename or metadata
                    detection_mode = self.config.get('schedule', {}).get('detection_mode', 'date_based')
                    if detection_mode == 'date_based':
                        # Build item dict with filename, href, and any extracted metadata
                        item_dict = {'filename': supplier_filename, 'href': href}
                        if 'modified' in link_item:
                            item_dict['modified'] = link_item['modified']
                            logger.info(f"[METADATA] Found 'modified' field in link_item: {link_item['modified']}", filename=supplier_filename)
                        else:
                            logger.warning(f"[METADATA] No 'modified' field in link_item for {supplier_filename}", link_item_keys=list(link_item.keys()))
                        
                        logger.info(f"[VERSION DETECTION] Calling version_detector with item_dict: {item_dict}", filename=supplier_filename)
                        version = self.version_detector.detect_version(
                            item=item_dict,
                            detection_mode=detection_mode
                        )
                        if version:
                            valid_from_date_str = version
                            logger.info(f"[VERSION DETECTED] Extracted date: {version}", filename=supplier_filename)
                        else:
                            logger.warning(f"[VERSION DETECTION FAILED] No version detected from item_dict: {item_dict}", filename=supplier_filename)
                    
                    logger.info(
                        f"[DUPLICATE CHECK] File {idx}/{len(filtered_links)}: supplier_filename={supplier_filename}, valid_from={valid_from_date_str}",
                        supplier_filename=supplier_filename,
                        valid_from_date=valid_from_date_str
                    )
                    
                    # Check if already processed (using supplier filename + date)
                    if supplier_filename and valid_from_date_str:
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename,
                            valid_from_date=valid_from_date_str
                        ):
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(filtered_links)} - Already have: {supplier_filename} valid from {valid_from_date_str}. NOT downloading.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                            self.files_skipped_duplicates += 1
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(filtered_links)} - Not in state: {supplier_filename} valid from {valid_from_date_str}. Will download.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                    elif supplier_filename:
                        # Fallback: check by filename if no date
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename
                        ):
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(filtered_links)} - Already have: {supplier_filename}. NOT downloading.",
                                supplier_filename=supplier_filename
                            )
                            self.files_skipped_duplicates += 1
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(filtered_links)} - Not in state: {supplier_filename}. Will download.",
                                supplier_filename=supplier_filename
                            )
                
                files_downloaded += 1
                logger.info(f"[DOWNLOAD] Downloading file {idx}/{len(filtered_links)} (file #{files_downloaded})")
                
                # Download
                file_path = await self._download_via_click(el)
                if not file_path:
                    continue
                
                # Generate normalized filename
                original_filename = os.path.basename(file_path)
                matched_brand_config = link_item.get('_matched_brand_config')
                raw_supplier_brand = matched_brand_config.get('brand') if matched_brand_config else link_item.get('_extracted_brand')
                
                normalized_filename = self._derive_filename(href, original_filename, raw_supplier_brand, valid_from_date_str)
                
                # Rename file to normalized name
                normalized_path = os.path.join(os.path.dirname(file_path), normalized_filename)
                if normalized_path != file_path:
                    os.replace(file_path, normalized_path)
                    file_path = normalized_path
                
                # Pass supplier_filename and valid_from_date_str for duplicate detection
                files.append(self.create_scraped_file(
                    normalized_filename, 
                    file_path, 
                    brand=raw_supplier_brand,
                    supplier_filename=supplier_filename,  # Original filename from website
                    valid_from_date_str=valid_from_date_str  # Date extracted from directory listing
                ))
                logger.info(f"Successfully saved file: {normalized_filename} (valid_from: {valid_from_date_str})")
            
            result.files = files
            result.success = len(files) > 0
        except Exception as e:
            logger.error(f"Link downloader scraping failed: {str(e)}")
            result.errors.append(str(e))
        finally:
            await self.browser_manager.close()
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    async def _authenticate(self) -> bool:
        auth_cfg = self.config.get('authentication', {})
        auth_method = auth_cfg.get('method')
        
        # HTTP Basic Auth is handled by browser context, no additional steps needed
        if auth_method == 'basic':
            logger.info("Using HTTP Basic Auth (credentials set in browser context)")
            return True
        
        # If not form auth, skip authentication
        if auth_method != 'form':
            return True
        login_url = auth_cfg.get('login_url')
        if login_url:
            ok = await self.browser_manager.navigate(login_url)
            if not ok:
                return False
        
        # Take screenshot before login
        await self.browser_manager.take_screenshot("00_before_login")
        
        # Try to dismiss cookie popups/banners that might block the form
        page = self.browser_manager._ensure_page_initialized()
        try:
            # Common cookie popup selectors
            cookie_selectors = [
                'button:has-text("Accept")',
                'button:has-text("Deny")',
                'button:has-text("OK")',
                'button:has-text("Agree")',
                'button:has-text("Close")',
                '[id*="cookie"] button',
                '[class*="cookie"] button',
                '.cookie-consent button',
                '#onetrust-accept-btn-handler',
                '.cc-dismiss',
                '.cookie-banner button'
            ]
            
            for selector in cookie_selectors:
                try:
                    cookie_button = await page.query_selector(selector)
                    if cookie_button and await cookie_button.is_visible():
                        await cookie_button.click()
                        logger.info(f"Dismissed cookie popup using selector: {selector}")
                        await page.wait_for_timeout(1000)
                        break
                except Exception:
                    continue
        except Exception as e:
            logger.warning(f"Could not dismiss cookie popup: {e}")
        
        # Retrieve and validate credentials using CredentialManager
        try:
            cred_manager = CredentialManager(self.supplier_name, auth_cfg)
            credentials = cred_manager.get_credentials()
            username = credentials.username
            password = credentials.password
        except CredentialError as e:
            logger.error(f"Credential validation failed for {self.supplier_name}: {e}")
            raise
        
        username_preview = username[:3] if username else 'N/A'
        logger.info(f"Attempting login with username: {username_preview}*** (password: {'present' if password else 'missing'})")
        
        # Fill form but DON'T submit yet
        result = await self.browser_manager.fill_form(
            username_field=auth_cfg.get('username_field'),
            password_field=auth_cfg.get('password_field'),
            username=username,
            password=password,
            submit_button=None  # Don't submit yet
        )
        
        # Check for CSRF tokens or hidden fields in the form
        page = self.browser_manager._ensure_page_initialized()
        try:
            hidden_fields = await page.evaluate("""
                () => {
                    const form = document.querySelector('form');
                    if (!form) return [];
                    
                    const hiddenInputs = form.querySelectorAll('input[type="hidden"]');
                    const fields = [];
                    hiddenInputs.forEach(input => {
                        fields.push({
                            name: input.name,
                            value: input.value ? input.value.substring(0, 50) + '...' : 'empty'
                        });
                    });
                    return fields;
                }
            """)
            
            if hidden_fields:
                logger.info(f"Found {len(hidden_fields)} hidden form fields:")
                for field in hidden_fields:
                    logger.info(f"  - {field['name']}: {field['value']}")
            else:
                logger.info("No hidden fields (CSRF tokens) found in form")
                
        except Exception as e:
            logger.warning(f"Could not check for hidden fields: {e}")
        
        # Take screenshot BEFORE submitting
        await self.browser_manager.take_screenshot("00_before_submit")
        
        # Now click the submit button
        page = self.browser_manager._ensure_page_initialized()
        submit_button_selector = auth_cfg.get('submit_button')
        
        logger.info(f"Submit button selector: {submit_button_selector}")
        
        # Check if form fields have validation errors BEFORE clicking submit
        try:
            # Check for HTML5 validation states
            username_field_selector = auth_cfg.get('username_field')
            password_field_selector = auth_cfg.get('password_field')
            
            # Use json.dumps to safely pass selectors with quotes
            username_valid = await page.evaluate(f"""
                () => {{
                    const field = document.querySelector({json.dumps(username_field_selector)});
                    return field ? field.validity.valid : true;
                }}
            """)
            password_valid = await page.evaluate(f"""
                () => {{
                    const field = document.querySelector({json.dumps(password_field_selector)});
                    return field ? field.validity.valid : true;
                }}
            """)
            
            logger.info(f"Form validation state - username valid: {username_valid}, password valid: {password_valid}")
            
            # Check if button is disabled
            button_disabled = await page.evaluate(f"""
                () => {{
                    const btn = document.querySelector({json.dumps(submit_button_selector)});
                    return btn ? btn.disabled : false;
                }}
            """)
            logger.info(f"Submit button disabled: {button_disabled}")
            
        except Exception as validation_err:
            logger.warning(f"Could not check form validation: {validation_err}")
        
        # Set up network monitoring to capture the submit request
        network_requests: List[Dict[str, Any]] = []
        network_responses: List[Dict[str, Any]] = []
        
        def log_request(request: Any) -> None:
            if 'login' in request.url.lower():
                network_requests.append({
                    'url': request.url,
                    'method': request.method,
                    'post_data': request.post_data if request.method == 'POST' else None
                })
                logger.info(f"Network Request: {request.method} {request.url}")
        
        def log_response(response: Any) -> None:
            if 'login' in response.url.lower():
                network_responses.append({
                    'url': response.url,
                    'status': response.status,
                    'status_text': response.status_text
                })
                logger.info(f"Network Response: {response.status} {response.status_text} from {response.url}")
        
        page.on('request', log_request)
        page.on('response', log_response)
        
        if submit_button_selector:
            try:
                # Check if button exists
                button_element = await page.query_selector(submit_button_selector)
                if button_element:
                    logger.info(f"Submit button found, clicking now...")
                    await button_element.click()
                    logger.info(f"Submit button clicked, waiting for response...")
                    
                    # Wait a moment to see if network traffic starts
                    await page.wait_for_timeout(2000)
                    
                    # If no network requests captured, try submitting the form directly via JavaScript
                    if len(network_requests) == 0:
                        logger.warning("No network request detected after button click, trying form.submit()")
                        try:
                            # Find the form and submit it directly
                            await page.evaluate("""
                                () => {
                                    const form = document.querySelector('form');
                                    if (form) {
                                        console.log('Submitting form via JavaScript');
                                        form.submit();
                                    }
                                }
                            """)
                            logger.info("Form submitted via JavaScript")
                            await page.wait_for_load_state('networkidle', timeout=60000)
                        except Exception as submit_err:
                            logger.error(f"JavaScript form submit failed: {submit_err}")
                    else:
                        await page.wait_for_load_state('networkidle', timeout=60000)
                    
                    logger.info(f"Page load complete after submit")
                    
                    # Log captured network traffic
                    logger.info(f"Captured {len(network_requests)} requests and {len(network_responses)} responses")
                    for req in network_requests:
                        logger.info(f"  Request: {req['method']} {req['url']}")
                    for resp in network_responses:
                        logger.info(f"  Response: {resp['status']} from {resp['url']}")
                else:
                    logger.error(f"Submit button not found with selector: {submit_button_selector}")
            except Exception as click_error:
                logger.error(f"Error clicking submit button: {click_error}")
        else:
            logger.warning("No submit button selector configured")
        
        # Take screenshot after login
        await self.browser_manager.take_screenshot("00_after_login")
        
        # Check URL to see if we're still on login page
        page = self.browser_manager._ensure_page_initialized()
        current_url = page.url
        logger.info(f"After login, current URL: {current_url}")
        
        # Check for error messages on the page
        try:
            error_selectors = [
                '.error', '.alert', '.alert-danger', '.text-danger', 
                '[class*="error"]', '[class*="invalid"]'
            ]
            for selector in error_selectors:
                error_elements = await page.query_selector_all(selector)
                for elem in error_elements:
                    error_text = await elem.inner_text()
                    if error_text and error_text.strip():
                        logger.error(f"Error message found: {error_text.strip()}")
        except Exception:
            pass
        
        if 'login' in current_url.lower():
            logger.error("Still on login page after authentication - login likely failed")
            logger.error("Please verify credentials manually at: https://wiuse.net/customer/login")
            return False
        
        # Remove network event handlers after successful authentication
        # to prevent logging noise from GA tracking and other requests during file downloads
        try:
            page.remove_listener('request', log_request)
            page.remove_listener('response', log_response)
            logger.info("Network monitoring disabled after successful authentication")
        except Exception as e:
            logger.debug(f"Could not remove network handlers (may not have been attached): {e}")
        
        return result
    
    def _extract_directory_listing_metadata(self, html_content: str) -> Dict[str, Dict[str, str]]:
        """
        Extract file metadata from HTML directory listing.
        
        Parses directory listings like:
        11/11/2025  9:58 PM        14405 <a href="...">BMW.csv</a><br>
        11/11/2025  9:58 PM      1046736 <a href="...">AS-PL.csv</a><br>
        
        Args:
            html_content: HTML content of the page
            
        Returns:
            Dictionary mapping filename to metadata dict with 'modified' key
        """
        metadata: Dict[str, Dict[str, str]] = {}
        
        # Pattern for FTP-style directory listing format: MM/DD/YYYY  HH:MM AM/PM  <size>  <a href="...">filename</a>
        # Matches: "11/11/2025  9:58 PM     12936370 <a href="/path/BMW.csv">BMW.csv</a>"
        # Made more flexible to handle varying whitespace between elements
        pattern = r'(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2})\s+(AM|PM)\s+\d+\s+<a[^>]*>([^<]+)</a>'
        
        matches = re.finditer(pattern, html_content, re.MULTILINE | re.IGNORECASE)
        
        matches_found = 0
        for match in matches:
            date_str = match.group(1)      # "11/11/2025"
            time_str = match.group(2)      # "9:58"
            ampm = match.group(3)          # "PM" or "AM"
            filename = match.group(4).strip()  # "BMW.csv"
            
            # Combine date and time with single space (normalized format)
            # This matches the format expected by version_detector: "MM/DD/YYYY HH:MM AM/PM"
            modified_str = f"{date_str} {time_str} {ampm}"
            
            metadata[filename] = {
                'modified': modified_str
            }
            matches_found += 1
            logger.info(f"Extracted directory metadata: {filename} -> modified={modified_str}")
        
        if metadata:
            logger.info(f"Successfully extracted metadata for {len(metadata)} files from FTP-style directory listing")
        else:
            # Log a sample of the HTML to debug why pattern didn't match
            logger.warning(f"No directory metadata extracted from HTML. Debugging info:")
            logger.warning(f"  HTML length: {len(html_content)} chars")
            logger.warning(f"  HTML sample (first 1000 chars): {html_content[:1000]}")
            
            # Try to find lines that look like they might be directory entries
            lines = html_content.split('<br>')
            logger.warning(f"  Found {len(lines)} lines separated by <br> tags")
            for i, line in enumerate(lines[:10]):  # Show first 10 lines
                logger.warning(f"  Line {i}: {line[:200]}")
        
        return metadata
    
    async def _filter_links_by_brand(self, link_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter links by matching brands against brand_config.json and config array.
        
        Filtering steps:
        1. Extract brand from each link's href
        2. Filter by config array if configured
        3. Match against brand_config.json
        
        Args:
            link_items: List of dicts with 'element' and 'href' keys
            
        Returns:
            Filtered list containing only links with matching brands
        """
        brand_from_url = self.config.get('brand_from_url', 'brandCode')
        
        # Extract active brands from config array (single source of truth)
        config_brands = self.config.get('config', [])
        active_brands = [
            cfg['brand'] 
            for cfg in config_brands 
            if cfg.get('enabled', True)  # enabled by default
        ]
        if active_brands:
            # Normalize to uppercase for comparison
            active_brands_upper = [b.upper() for b in active_brands]
            logger.info(f"Filtering by config brands: {', '.join(active_brands)}")
        
        # Load brand configs once
        # Note: load_brand_configs() will return the cached config set by orchestrator
        brand_configs = load_brand_configs()
        if not brand_configs:
            logger.error("Failed to load brand configs - cannot filter links")
            return []
        
        filtered_links: List[Dict[str, Any]] = []
        skipped_brands: Dict[str, int] = {}
        matched_brands: Dict[str, int] = {}
        config_filtered_brands: Dict[str, int] = {}
        
        for link_item in link_items:
            href = link_item['href']
            
            # Extract brand from URL
            extracted_brand = self._infer_brand_from_href(href)
            if not extracted_brand:
                logger.warning(f"Could not extract brand from href: {href}")
                continue
            
            # Strip numeric suffixes (e.g., "VW_1" -> "VW", "BMW_2" -> "BMW")
            # This allows files like "VW_1.csv" and "VW.csv" to both match "VW"
            base_brand = re.sub(r'_\d+$', '', extracted_brand)
            if base_brand != extracted_brand:
                logger.debug(f"Stripped numeric suffix from brand: {extracted_brand} -> {base_brand}")
            
            # First filter: Check config array filter if configured
            # Use base_brand for matching, but keep original extracted_brand for reference
            if active_brands:
                if base_brand.upper() not in active_brands_upper:
                    # Brand not in config array, skip
                    config_filtered_brands[extracted_brand] = config_filtered_brands.get(extracted_brand, 0) + 1
                    continue
            
            # Second filter: Match against brand configs
            # Try base_brand first, fall back to extracted_brand if no match
            matched_config = find_matching_brand(base_brand, brand_configs)
            if not matched_config:
                # Fallback to original extracted_brand
                matched_config = find_matching_brand(extracted_brand, brand_configs)
            
            if matched_config:
                # Store the matched brand config in the link for later use
                link_item['_matched_brand_config'] = matched_config
                link_item['_extracted_brand'] = extracted_brand
                filtered_links.append(link_item)
                
                # Track matched brands
                brand_name = matched_config.get('brand', extracted_brand)
                matched_brands[brand_name] = matched_brands.get(brand_name, 0) + 1
            else:
                # Track skipped brands
                skipped_brands[extracted_brand] = skipped_brands.get(extracted_brand, 0) + 1
        
        # Log filtering results
        logger.info(f"Brand filtering: {len(filtered_links)}/{len(link_items)} links matched")
        
        if config_filtered_brands:
            filtered_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(config_filtered_brands.items())])
            logger.info(f"Filtered by config array: {filtered_summary}")
        
        if matched_brands:
            matched_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(matched_brands.items())])
            logger.info(f"Matched brands: {matched_summary}")
        
        if skipped_brands:
            skipped_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(skipped_brands.items())])
            logger.warning(f"Skipped brands (not in brand_config.json): {skipped_summary}")
        
        return filtered_links
    
    async def _download_via_click(self, element: Any) -> Optional[str]:
        try:
            page = self.browser_manager._ensure_page_initialized()
            
            # Log element details before attempting download
            href = await element.get_attribute('href')
            text = await element.inner_text()
            is_visible = await element.is_visible()
            logger.info(f"[DOWNLOAD START] Preparing to download file")
            logger.info(f"  Element href: {href}")
            logger.info(f"  Element text: {text[:50] if text else 'N/A'}")
            logger.info(f"  Element visible: {is_visible}")
            logger.info(f"  Current page URL: {page.url}")
            
            # Check if element is clickable
            try:
                await element.wait_for_element_state('enabled', timeout=5000)
                logger.info(f"  Element is enabled and clickable")
            except Exception as clickable_err:
                logger.warning(f"  Element may not be clickable: {clickable_err}")
            
            # Log before waiting for download
            logger.info(f"[DOWNLOAD WAIT] Setting up download listener (60s timeout)...")
            
            try:
                async with page.expect_download(timeout=60000) as download_info:
                    logger.info(f"[DOWNLOAD CLICK] Clicking element now...")
                    await element.click()
                    logger.info(f"[DOWNLOAD CLICK] Click completed, waiting for download event...")
                
                # Log when download event fires
                logger.info(f"[DOWNLOAD EVENT] Download event received")
                download = await download_info.value
                
                # Log download details
                suggested_filename = download.suggested_filename or 'downloaded_file'
                logger.info(f"[DOWNLOAD SAVE] Saving file: {suggested_filename}")
                
                file_path = os.path.join(self.browser_manager.download_dir, suggested_filename)
                await download.save_as(file_path)
                
                # Log file size after save
                file_size = os.path.getsize(file_path)
                logger.info(f"[DOWNLOAD COMPLETE] File saved: {suggested_filename} ({file_size} bytes)")
                
                return file_path
                
            except TimeoutError as timeout_err:
                # Specific timeout error handling
                logger.error(f"[DOWNLOAD TIMEOUT] Download did not start within 60 seconds")
                logger.error(f"  This usually means:")
                logger.error(f"    - Click didn't trigger download")
                logger.error(f"    - Link navigates to page instead of downloading")
                logger.error(f"    - JavaScript required for download")
                logger.error(f"  Element href was: {href}")
                
                # Check download directory to see if anything was downloaded
                logger.info(f"Checking download directory: {self.browser_manager.download_dir}")
                downloaded_files = os.listdir(self.browser_manager.download_dir)
                logger.info(f"Files in download dir: {downloaded_files}")
                
                raise
                
        except Exception as e:
            logger.error(f"[DOWNLOAD FAILED] Download via click failed: {str(e)}")
            logger.error(f"  Exception type: {type(e).__name__}")
            
            # Check download directory on any error
            try:
                logger.info(f"Checking download directory: {self.browser_manager.download_dir}")
                downloaded_files = os.listdir(self.browser_manager.download_dir)
                logger.info(f"Files in download dir: {downloaded_files}")
            except Exception as dir_err:
                logger.warning(f"Could not check download directory: {dir_err}")
            
            return None
    
    def _derive_filename(self, href: str, original_filename: str, brand: Optional[str], valid_from_date_str: Optional[str] = None) -> str:
        """
        Derive normalized filename from href and brand.
        
        Format: {Brand}_{SupplierName}_{Location}_{Currency}_{DateMMYY}.{ext}
        
        Args:
            href: Link href
            original_filename: Original filename from download
            brand: Brand name
            valid_from_date_str: Optional ISO date string (e.g., "2025-12-31T21:56:00") to use for date part
        """
        # Get extension from original filename
        _, ext = os.path.splitext(original_filename)
        if not ext:
            ext = '.xlsx'  # Default extension
        
        # Get location and currency from top-level config
        location = self.config.get('location', '')
        currency = self.config.get('currency', '')
        
        # Normalize brand name
        normalized_brand = normalize_brand(brand) if brand else 'UNKNOWN'
        
        # Extract date part (MMYY format)
        date_part: str
        if valid_from_date_str:
            try:
                # Parse ISO format date string (e.g., "2025-12-31T21:56:00")
                # Try ISO format first
                try:
                    dt = datetime.fromisoformat(valid_from_date_str.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    # Try parsing with dateutil if available
                    try:
                        from dateutil import parser as date_parser
                        dt = date_parser.parse(valid_from_date_str)
                    except Exception:
                        # Fallback to current date
                        dt = datetime.now()
                date_part = dt.strftime('%m%y')
                logger.debug(f"Using extracted date for filename: {valid_from_date_str} -> {date_part}")
            except Exception as e:
                logger.warning(f"Failed to parse valid_from_date_str '{valid_from_date_str}', using current date: {e}")
                date_part = datetime.now().strftime('%m%y')
        else:
            # No date provided, use current date
            date_part = datetime.now().strftime('%m%y')
        
        # Build filename: {Brand}_{SupplierName}_{Location}_{Currency}_{DateMMYY}.{ext}
        parts = [normalized_brand, self.supplier_name]
        if location:
            parts.append(location)
        if currency:
            parts.append(currency)
        parts.append(date_part)
        
        return f"{'_'.join(parts)}{ext}"
    
    def _infer_brand_from_href(self, href: str) -> Optional[str]:
        """
        Extract brand from href based on config.
        
        Supports:
        - New brand_detection config with source/pattern
        - Legacy brand_from_filename + brand_pattern
        - URL query params
        """
        try:
            from urllib.parse import unquote
            
            # NEW: Check for brand_detection config first
            brand_detection = self.config.get('brand_detection', {})
            if brand_detection:
                source = brand_detection.get('source', 'url')
                pattern = brand_detection.get('pattern', '^([A-Z0-9_-]+)')
                
                if source == 'filename':
                    # Extract filename from path, then match
                    parsed = urlparse(href)
                    filename = os.path.basename(parsed.path)
                    filename = unquote(filename)  # Decode %20 etc
                    
                    match = re.search(pattern, filename)
                    if match:
                        brand = match.group(1).strip()
                        return brand.upper()
                    else:
                        return None
                elif source == 'url':
                    # Match against full href
                    match = re.search(pattern, href)
                    if match:
                        brand = match.group(1).strip()
                        return brand.upper()
            
            # LEGACY: Fallback to old config for backwards compatibility
            if self.config.get('brand_from_filename'):
                # Extract filename from URL path
                parsed = urlparse(href)
                filename = os.path.basename(parsed.path)
                # URL-decode to handle %20 spaces, etc.
                filename = unquote(filename)
                
                # Apply brand pattern if configured
                brand_pattern = self.config.get('brand_pattern', r'^([^.]+)')  # Default: everything before first dot
                match = re.search(brand_pattern, filename)
                if match:
                    brand = match.group(1).strip()
                    return brand.upper()
            
            # Fallback to query parameter extraction (for link-based downloads)
            brand_from_url_param = self.config.get('brand_from_url', 'brandCode')
            q = parse_qs(urlparse(href).query)
            brand_code = (q.get(brand_from_url_param) or q.get('brand') or q.get('brandCode') or [''])[0]
            if brand_code:
                return brand_code.upper()
            
            return None
        except Exception as e:
            logger.debug(f"Failed to extract brand from href {href}: {e}")
            return None
    
    # Abstract method implementations (not used, scrape() does everything)
    async def authenticate(self) -> bool:
        """Stub implementation - authentication is handled in scrape()."""
        return await self._authenticate()
    
    async def navigate_to_downloads(self) -> bool:
        """Stub implementation - navigation is handled in scrape()."""
        return True
    
    async def download_files(self) -> List[ScrapedFile]:
        """Stub implementation - downloading is handled in scrape()."""
        return []


