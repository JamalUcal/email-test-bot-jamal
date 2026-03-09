"""
AUTOCAR supplier scraper.

Logs in via form to dashboard.autocar.nl, discovers available price files from
an HTML table, and downloads JSON data via a button-driven endpoint that
returns an array-of-arrays (masquerading as .xlsx). Implements streaming with
duplicate detection and brand filtering.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

import httpx

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import (
    extract_brand_from_text,
    extract_config_brand,
    find_matching_brand,
    load_brand_configs,
    normalize_brand,
)
from utils.logger import get_logger


logger = get_logger(__name__)


class AutocarScraper(BaseScraper):
    """
    Custom scraper for AUTOCAR.

    Page structure (after login):
    - Download page: https://dashboard.autocar.nl/customer/pricefiles/index
    - Rows contain a button with class 'download-price-file' and attributes:
      - data-name: e.g., autocar_international_FORD_22_10_2025.xlsx
      - data-list-id: e.g., 7104
    - Download URL pattern:
      https://dashboard.autocar.nl/customer/pricefiles/download/?list=<id>&_=<timestamp_ms>
    - Response is JSON array-of-arrays, first row is header
    """

    async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
        try:
            await self.browser_manager.start()
            auth_ok = await self.authenticate()
            if not auth_ok:
                logger.error("Authentication failed for AUTOCAR")
                raise Exception("Authentication failed for AUTOCAR")

            nav_ok = await self.navigate_to_downloads()
            if not nav_ok:
                logger.error("Navigation to downloads failed for AUTOCAR")
                raise Exception("Navigation to downloads failed for AUTOCAR")
            
            logger.info(f"✓ Navigation to downloads page completed")
            page = self.browser_manager._ensure_page_initialized()
            current_url = page.url
            page_title = await page.title()
            logger.info(f"Current page URL: {current_url}")
            logger.info(f"Current page title: {page_title}")
            
            # Verify we actually reached the downloads page
            expected_path = "customer/pricefiles/index"
            if expected_path not in current_url:
                logger.error(f"❌ Not on downloads page! Expected '{expected_path}' in URL")
                logger.error(f"Actual URL: {current_url}")
                if 'login' in current_url.lower():
                    logger.error("🔴 CRITICAL: Redirected back to login - session not established!")
                
                # Take screenshot to see what page we're actually on
                await self.browser_manager.take_screenshot("autocar_FAILED_not_on_downloads_page")
                logger.error("📸 Screenshot saved: autocar_FAILED_not_on_downloads_page")
                
                raise Exception(f"Failed to reach downloads page - at {current_url}")

            # Harvest cookies for httpx client
            client = await self._prepare_http_client()

            # Discover rows with buttons
            items = await self._discover_items()
            if not items:
                logger.warning("No downloadable items discovered on AUTOCAR page")
                return

            # Load brand configs and build enabled brands list from config array
            # Note: load_brand_configs() will return the cached config set by orchestrator
            brand_configs = load_brand_configs()
            config_array = self.config.get('config', [])
            enabled_upper: List[str] = [item['brand'].upper() for item in config_array if 'brand' in item]

            # Track file counts for duplicate detection reporting
            self.total_files_found = 0
            self.files_skipped_duplicates = 0
            
            files_downloaded = 0
            for idx, item in enumerate(items, 1):
                if idx <= self.start_index:
                    logger.info(
                        f"[RESUME] Skipping item {idx}/{len(items)} (resuming from index {self.start_index})"
                    )
                    continue

                supplier_filename = item['data_name']
                extracted_brand = item['brand']
                valid_from_str = item.get('valid_from')

                # Use extract_config_brand to find the base brand (handles progressive segment removal)
                # e.g., "VOLVO SPECIAL" -> tries "VOLVO SPECIAL", then "VOLVO" -> matches "VOLVO"
                config_brand = extract_config_brand(extracted_brand, brand_configs)
                if not config_brand:
                    logger.warning(
                        f"[BRAND SKIP] No brand_config match for '{extracted_brand}' - skipping {supplier_filename}"
                    )
                    continue

                # Filter by config brands using the config brand (base brand name)
                if enabled_upper and config_brand.upper() not in enabled_upper:
                    logger.debug(
                        f"[BRAND FILTER] Skipping {supplier_filename} - brand {config_brand} (from {extracted_brand}) not enabled"
                    )
                    continue

                # Get matched brand config for downstream processing
                matched_brand = find_matching_brand(config_brand, brand_configs)
                if not matched_brand:
                    logger.warning(
                        f"[BRAND SKIP] No brand_config found for config brand '{config_brand}' - skipping {supplier_filename}"
                    )
                    continue

                # Count this file (passed brand filtering)
                self.total_files_found += 1

                # Duplicate detection before download
                if self.state_manager:
                    if self.state_manager.is_file_already_processed(
                        supplier=self.supplier_name,
                        supplier_filename=supplier_filename,
                        valid_from_date=valid_from_str,
                    ):
                        logger.info(
                            f"[DUPLICATE SKIP] Already have {supplier_filename} valid from {valid_from_str} - skipping download"
                        )
                        self.files_skipped_duplicates += 1
                        continue

                # Download via API with cookies
                files_downloaded += 1
                logger.info(
                    f"[DOWNLOAD] {idx}/{len(items)} (file #{files_downloaded}) - {supplier_filename}"
                )
                scraped = await self._download_item(
                    client=client,
                    list_id=item['list_id'],
                    supplier_filename=supplier_filename,
                    raw_supplier_brand=extracted_brand,
                    valid_from_str=valid_from_str,
                )
                if scraped:
                    yield scraped

        except Exception as e:
            logger.error(f"AUTOCAR scrape_stream failed: {e}")
            raise  # Re-raise to allow orchestrator to capture the error
        finally:
            await self.browser_manager.close()

    async def scrape(self) -> ScrapingResult:
        import time as _time
        start = _time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        try:
            files: List[ScrapedFile] = []
            async for f in self.scrape_stream():
                files.append(f)
            result.files = files
            result.success = len(files) > 0
        except Exception as e:
            result.errors.append(str(e))
        finally:
            result.execution_time_seconds = _time.time() - start
        return result

    async def authenticate(self) -> bool:
        auth = self.config.get('authentication', {})
        login_url: Optional[str] = auth.get('login_url')
        username_field: Optional[str] = auth.get('username_field')
        password_field: Optional[str] = auth.get('password_field')
        submit_button: Optional[str] = auth.get('submit_button')

        if not login_url or not username_field or not password_field:
            logger.error("Missing authentication configuration for AUTOCAR")
            return False

        ok = await self.browser_manager.navigate(login_url)
        if not ok:
            return False
        
        # Wait for login page JavaScript to fully load and populate hidden fields (CSRF token)
        page = self.browser_manager._ensure_page_initialized()
        
        # Check for 404 or other error pages before attempting login
        page_title = await page.title()
        current_url = page.url
        if '404' in page_title or 'not found' in page_title.lower():
            error_msg = f"Login page returned 404: {current_url} (title: {page_title})"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        is_cloud = bool(os.getenv('FUNCTION_TARGET') or os.getenv('K_SERVICE'))
        idle_timeout = 30000 if is_cloud else 10000  # 30s for Cloud Run, 10s for local
        
        logger.info(f"Waiting for login page JavaScript to initialize (networkidle, {idle_timeout/1000}s timeout)...")
        try:
            await page.wait_for_load_state('networkidle', timeout=idle_timeout)
            logger.info("Login page JavaScript fully loaded (network idle)")
        except Exception as e:
            logger.warning(f"Network idle timeout: {e} - proceeding anyway")
            # Still proceed - page might be functional even if not fully idle

        username = os.getenv(auth.get('username_env', ''), '')
        password = os.getenv(auth.get('password_env', ''), '')
        if not username or not password:
            logger.error("AUTOCAR credentials not found in environment")
            return False

        await self.browser_manager.take_screenshot("autocar_00_login")
        
        # Extract and verify CSRF token / form key before submitting
        # AUTOCAR uses "form_key" hidden field for CSRF protection
        # (page already initialized above)
        
        # DEBUG: Check cookies BEFORE form submission
        cookies_before = await page.context.cookies()
        logger.info(f"Cookies before form submission: {len(cookies_before)} cookies")
        for cookie in cookies_before:
            cookie_value = cookie.get('value')
            value_preview = cookie_value[:20] if cookie_value else 'None'
            logger.info(f"  Cookie: {cookie.get('name')}={value_preview}... (domain={cookie.get('domain')}, secure={cookie.get('secure')})")
        
        form_key_value = None
        try:
            form_key_input = await page.query_selector('input[name="form_key"]')
            if form_key_input:
                form_key_value = await form_key_input.get_attribute('value')
                if form_key_value:
                    logger.info(f"Found form_key (CSRF token): {form_key_value[:20]}...")
                else:
                    logger.warning("⚠️ form_key field exists but value is empty!")
            else:
                logger.warning("⚠️ No form_key found in login form - may cause 'Invalid form Key' error")
        except Exception as e:
            logger.error(f"❌ Could not extract form_key: {e}")
        
        # DEBUG: Check what the submit button actually is
        if submit_button:
            submit_elem = await page.query_selector(submit_button)
            if submit_elem:
                submit_tag = await submit_elem.evaluate("el => el.tagName")
                submit_type = await submit_elem.evaluate("el => el.type || 'N/A'")
                submit_onclick = await submit_elem.evaluate("el => el.onclick ? 'HAS_ONCLICK' : 'NO_ONCLICK'")
                logger.info(f"Submit button: tag={submit_tag}, type={submit_type}, onclick={submit_onclick}")
        
        # Check form action to see where it submits
        try:
            form = await page.query_selector('form')
            if form:
                form_action = await form.evaluate("el => el.action")
                form_method = await form.evaluate("el => el.method")
                logger.info(f"Form action: {form_action}, method: {form_method}")
        except Exception as e:
            logger.warning(f"Could not inspect form: {e}")
        
        # Monitor network requests to see if POST happens
        post_requests: List[str] = []
        post_responses: List[Tuple[str, int, str]] = []  # (url, status, redirect)
        
        def track_request(request):
            if request.method == "POST":
                post_requests.append(f"{request.method} {request.url}")
                # Log POST headers
                headers = request.headers
                logger.info(f"POST Headers: User-Agent={headers.get('user-agent', 'N/A')[:50]}")
                
                # Log POST body contents (redact password for security)
                try:
                    post_data = request.post_data
                    if post_data:
                        # Parse the POST data to redact password
                        import urllib.parse
                        parsed_data = urllib.parse.parse_qs(post_data)
                        
                        # Create safe version with password redacted
                        safe_data = {}
                        for key, values in parsed_data.items():
                            if 'password' in key.lower():
                                safe_data[key] = ['[REDACTED]']
                            else:
                                safe_data[key] = [v[:50] + '...' if len(v) > 50 else v for v in values]
                        
                        logger.info(f"POST Body Fields: {list(safe_data.keys())}")
                        for key, values in safe_data.items():
                            logger.info(f"  {key}: {values[0]}")
                    else:
                        logger.warning("POST body is empty!")
                except Exception as e:
                    logger.warning(f"Could not parse POST body: {e}")
        
        def track_response(response):
            if response.request.method == "POST":
                status = response.status
                redirect = response.headers.get('location', 'NO_REDIRECT')
                post_responses.append((response.url, status, redirect))
                logger.info(f"POST Response: status={status}, redirect={redirect}")
                
                # Log response headers (especially Set-Cookie)
                # Note: Playwright headers are a dict, not multidict, so use get() instead of get_all()
                set_cookie_header = response.headers.get('set-cookie')
                if set_cookie_header:
                    # Playwright may combine multiple Set-Cookie headers or return the first one
                    cookie_name = set_cookie_header.split('=')[0] if '=' in set_cookie_header else 'unknown'
                    logger.info(f"  Set-Cookie: {cookie_name}...")
                else:
                    logger.warning("⚠️ Server did NOT send any Set-Cookie headers!")
        
        page.on("request", track_request)
        page.on("response", track_response)
        logger.info("🔍 Monitoring POST requests during form submission...")
        
        # Fill form and submit (Playwright will automatically include hidden fields like form_key)
        form_filled = await self.browser_manager.fill_form(
            username_field=username_field,
            password_field=password_field,
            username=username,
            password=password,
            submit_button=submit_button,
        )
        
        if not form_filled:
            error_msg = "Failed to fill login form - form fill timed out or failed"
            logger.error(error_msg)
            raise Exception(error_msg)
        
        # Verify form_key was actually present before submission
        if not form_key_value:
            logger.error("❌ CRITICAL: Form submitted without CSRF token - authentication will likely fail with 'Invalid form Key'")
        
        # Check what POST requests were made
        page.remove_listener("request", track_request)
        page.remove_listener("response", track_response)
        
        if post_requests:
            logger.info(f"✅ POST requests detected during form submit: {len(post_requests)}")
            for req in post_requests:
                logger.info(f"  - {req}")
            
            # Analyze POST response
            if post_responses:
                for url, status, redirect in post_responses:
                    logger.info(f"POST to {url}: HTTP {status}")
                    if redirect != 'NO_REDIRECT':
                        logger.info(f"  → Redirect to: {redirect}")
                    elif status == 200:
                        logger.warning(f"⚠️ POST returned 200 (no redirect) - usually means validation error!")
        else:
            logger.error("❌ NO POST requests detected! Form didn't actually submit!")
            logger.error("This means JavaScript validation blocked submission or form is AJAX-based")
        
        # Take screenshot IMMEDIATELY after login POST (before navigating away)
        await self.browser_manager.take_screenshot("autocar_01_after_login")
        
        # DEBUG: Check cookies AFTER form submission
        cookies_after = await page.context.cookies()
        logger.info(f"Cookies after form submission: {len(cookies_after)} cookies")
        cookie_names_before = {c.get('name') for c in cookies_before}
        cookie_names_after = {c.get('name') for c in cookies_after}
        new_cookies = cookie_names_after - cookie_names_before
        if new_cookies:
            logger.info(f"New cookies set after login: {new_cookies}")
        else:
            logger.warning("⚠️ No new cookies set after login - session may not be established")
        
        # Verify login succeeded by checking URL and page content
        page = self.browser_manager._ensure_page_initialized()
        current_url = page.url
        page_title = await page.title()
        
        logger.info(f"After login - URL: {current_url}")
        logger.info(f"After login - Title: {page_title}")
        
        # Check if still on login page (authentication failed)
        if 'login' in current_url.lower():
            logger.error("❌ Authentication failed: Still on login page after form submission")
            logger.error(f"Expected to be redirected, but stayed at: {current_url}")
            return False
        
        # Check for authenticated session indicators
        try:
            # Look for elements that only appear when logged in
            logout_button = await page.query_selector('a[href*="logout"], button[href*="logout"], .logout')
            user_menu = await page.query_selector('.user-menu, .account-menu, [data-user]')
            
            if logout_button or user_menu:
                logger.info("✓ Authentication verified: Found logout button or user menu")
                return True
            else:
                logger.warning("⚠ Authentication unclear: No logout button or user menu found")
                # Don't fail yet - some sites don't have obvious indicators
                return True
        except Exception as check_error:
            logger.warning(f"Could not verify authentication elements: {check_error}")
            return True

    async def navigate_to_downloads(self) -> bool:
        page_url: str = self.config.get('links', {}).get(
            'page_url', 'https://dashboard.autocar.nl/customer/pricefiles/index'
        )
        nav_ok = await self.browser_manager.navigate(page_url)
        
        if nav_ok:
            # Wait for network to be idle (all resources loaded, JavaScript executed)
            # Cloud Run is much slower, so use longer timeout
            page = self.browser_manager._ensure_page_initialized()
            
            import os
            is_cloud = bool(os.getenv('FUNCTION_TARGET') or os.getenv('K_SERVICE'))
            network_timeout = 30000 if is_cloud else 10000  # 30s Cloud, 10s Local
            
            logger.info(f"Waiting for network idle ({network_timeout/1000}s timeout)...")
            
            try:
                start_wait = time.time()
                await page.wait_for_load_state('networkidle', timeout=network_timeout)
                elapsed_wait = time.time() - start_wait
                logger.info(f"Network idle after {elapsed_wait:.2f}s - page fully loaded")
            except Exception as wait_error:
                logger.warning(f"Network idle wait timeout after {network_timeout/1000}s: {wait_error}")
                logger.warning("Proceeding anyway, page may not be fully loaded")
        
        return nav_ok

    async def download_files(self) -> List[ScrapedFile]:
        # Not used; streaming implementation is preferred
        return []

    async def _prepare_http_client(self) -> httpx.Client:
        """
        Prepare an httpx client reusing cookies from the logged-in browser context.
        """
        page = self.browser_manager._ensure_page_initialized()
        cookies = await page.context.cookies()
        cookie_map: Dict[str, str] = {}
        for c in cookies:
            name = c.get('name')
            value = c.get('value')
            if name and value:
                cookie_map[name] = value

        client = httpx.Client(
            base_url="https://dashboard.autocar.nl",
            cookies=cookie_map,
            timeout=60.0,
            follow_redirects=True,
        )
        return client

    async def _discover_items(self) -> List[Dict[str, str]]:
        """
        Parse table rows and return a list of items with list_id, data_name, brand, valid_from.
        """
        page = self.browser_manager._ensure_page_initialized()
        
        # Wait for download buttons to be visible (environment-aware timeout)
        try:
            # Use longer timeout in Cloud Run environment
            import os
            is_cloud = bool(os.getenv('FUNCTION_TARGET') or os.getenv('K_SERVICE'))
            selector_timeout = 60000 if is_cloud else 30000
            logger.info(f"Waiting for download buttons ({selector_timeout/1000}s timeout)...")
            start_time = time.time()
            await page.wait_for_selector("button.download-price-file", timeout=selector_timeout, state="visible")
            elapsed = time.time() - start_time
            logger.info(f"Download buttons found after {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ TIMEOUT after {elapsed:.1f}s waiting for download buttons")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Exception message: {str(e)}")
            logger.error(f"Current page URL: {page.url}")
            logger.error(f"Current page title: {await page.title()}")
            
            # Take screenshot to see what page looks like when timeout occurs
            await self.browser_manager.take_screenshot("autocar_TIMEOUT_waiting_for_buttons")
            logger.error("📸 Screenshot saved: autocar_TIMEOUT_waiting_for_buttons")
            
            # Check if we're still on login page
            if 'login' in page.url.lower():
                logger.error("🔴 CRITICAL: Still on login page - authentication failed!")
            else:
                logger.error(f"⚠️ Not on login page, but buttons not found. Check if page needs more time to load.")
            
            return []
        
        rows = await page.query_selector_all("tr:has(button.download-price-file)")
        items: List[Dict[str, str]] = []
        for row in rows:
            try:
                button = await row.query_selector("button.download-price-file")
                if not button:
                    continue
                data_name = await button.get_attribute("data-name")
                list_id = await button.get_attribute("data-list-id")
                if not data_name or not list_id:
                    continue

                brand, valid_from = self._parse_filename_for_brand_and_date(data_name)

                items.append(
                    {
                        'list_id': list_id,
                        'data_name': data_name,
                        'brand': brand or '',
                        'valid_from': valid_from or '',
                    }
                )
            except Exception:
                continue
        logger.info(f"Discovered {len(items)} downloadable items on AUTOCAR page")
        return items

    def _parse_filename_for_brand_and_date(self, filename: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract brand and valid-from date from filenames like:
        autocar_international_FORD_22_10_2025.xlsx
        autocar_international_VOLVO SPECIAL_21_10_2025.xlsx
        
        The pattern is: autocar_international_{BRAND}_{DD}_{MM}_{YYYY}.xlsx
        The brand can be multi-word (e.g., "VOLVO SPECIAL").
        
        Returns (brand, date_iso) where:
        - brand: Full brand string from filename (e.g., "VOLVO SPECIAL" or "FORD")
        - date_iso: ISO date string 'YYYY-MM-DD'
        
        Note: The brand extraction preserves the full string. The progressive
        segment removal logic (to find base brand like "VOLVO" from "VOLVO SPECIAL")
        is handled by extract_config_brand() in the calling code.
        """
        # Extract date first (DD_MM_YYYY pattern)
        date_match = re.search(r"_(\d{2})_(\d{2})_(\d{4})", filename)
        valid_from: Optional[str] = None
        if date_match:
            dd, mm, yyyy = date_match.group(1), date_match.group(2), date_match.group(3)
            try:
                dt = datetime(int(yyyy), int(mm), int(dd), tzinfo=timezone.utc)
                valid_from = dt.date().isoformat()
            except Exception:
                pass
        
        # Extract brand using the existing utility function
        # Pattern: everything after "international_" and before "_DD_MM_YYYY"
        brand_pattern = r"international_(.+?)_\d{2}_\d{2}_\d{4}"
        brand = extract_brand_from_text(filename, brand_pattern)
        
        if brand:
            # extract_brand_from_text returns the captured group, normalize to uppercase
            brand = brand.upper().strip()
            return brand, valid_from
        
        return None, valid_from

    async def _download_item(
        self,
        client: httpx.Client,
        list_id: str,
        supplier_filename: str,
        raw_supplier_brand: str,
        valid_from_str: Optional[str],
    ) -> Optional[ScrapedFile]:
        """
        Call the JSON download endpoint and save data to a .xlsx-named JSON file.
        """
        # Compose URL with timestamp to avoid caching
        ts_ms = int(time.time() * 1000)
        resp = client.get(
            "/customer/pricefiles/download/",
            params={"list": list_id, "_": ts_ms},
            headers={"Accept": "application/json, */*"},
        )
        resp.raise_for_status()

        # The response is JSON array-of-arrays; save as JSON but with .xlsx extension
        data = resp.json()
        # Ensure it's a list
        if not isinstance(data, list):
            logger.warning("Unexpected response format from AUTOCAR; saving raw content")
        
        # Build normalized filename: {Brand}_{Supplier}_{Location}_{Currency}_{MMYY}.xlsx
        location = self.config.get('location', '')
        currency = self.config.get('currency', '')
        normalized_supplier_brand = normalize_brand(raw_supplier_brand) if raw_supplier_brand else 'UNKNOWN'
        filename = "_".join(
            [
                normalized_supplier_brand,
                self.supplier_name,
                *( [location] if location else [] ),
                *( [currency] if currency else [] ),
                datetime.now().strftime('%m%y'),
            ]
        ) + ".xlsx"

        # Choose download directory
        download_dir = getattr(self.browser_manager, 'download_dir', None) or os.getcwd()
        os.makedirs(download_dir, exist_ok=True)
        file_path = os.path.join(download_dir, filename)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to save AUTOCAR JSON data: {e}")
            return None

        scraped_kwargs: Dict[str, Any] = {
            'brand': raw_supplier_brand,
            'supplier_filename': supplier_filename,
        }
        if valid_from_str:
            scraped_kwargs['valid_from_date_str'] = valid_from_str

        return self.create_scraped_file(
            filename=filename,
            local_path=file_path,
            **scraped_kwargs,
        )


