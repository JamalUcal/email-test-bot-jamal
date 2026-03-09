"""
NEOPARTA Scraper - Hybrid browser login + REST API client.

This scraper uses browser automation to log in and harvest authentication tokens,
then uses those tokens to make REST API calls for downloading price lists.

Authentication flow:
1. Browser opens login page
2. Fill username/password form
3. Submit form and wait for success
4. Harvest cookies and/or localStorage token
5. Use harvested auth for subsequent HTTP API calls
"""

from typing import List, Dict, Any, Optional, cast, AsyncIterator
from datetime import datetime, timezone
import os
import re
import tempfile

import httpx

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import extract_brand_from_text, normalize_brand, find_matching_brand, load_brand_configs
from parsers.field_name_detector import FieldNameDetector
from utils.logger import get_logger
from utils.credential_manager import CredentialManager

logger = get_logger(__name__)


class NeopartaScraper(BaseScraper):
    """
    Hybrid scraper for NEOPARTA that combines browser authentication with API calls.
    
    This scraper:
    1. Uses Playwright to perform browser-based login
    2. Harvests cookies and/or localStorage tokens from the browser session
    3. Uses httpx to make authenticated HTTP API requests
    4. Supports list+export flow for downloading price lists
    """
    
    def __init__(
        self,
        scraper_config: Dict[str, Any],
        browser_manager: Any,
        start_index: int = 0,
        state_manager: Optional[Any] = None
    ):
        """
        Initialize the NEOPARTA scraper.
        
        Args:
            scraper_config: Supplier-specific scraping configuration
            browser_manager: BrowserManager instance for browser automation
            start_index: Index to resume from (for interrupted runs)
            state_manager: StateManager for duplicate detection
        """
        super().__init__(scraper_config, browser_manager, start_index, state_manager)
        self.credential_manager = CredentialManager(
            supplier_name=scraper_config['supplier'],
            auth_config=scraper_config.get('authentication', {})
        )
        self.session_cookies: Dict[str, str] = {}
        self.session_headers: Dict[str, str] = {}
        # Track file counts for duplicate detection reporting
        self.total_files_found: int = 0
        self.files_skipped_duplicates: int = 0
        
        # Initialize field name detector for API response field detection
        from pathlib import Path
        import json
        column_mapping_path = Path("config/core/column_mapping_config.json")
        if column_mapping_path.exists():
            with open(column_mapping_path, 'r') as f:
                column_mapping_config = json.load(f)
            self.field_name_detector: Optional[FieldNameDetector] = FieldNameDetector(column_mapping_config)
            logger.debug("Initialized FieldNameDetector for API field detection")
        else:
            self.field_name_detector = None
            logger.warning("column_mapping_config.json not found - field detection unavailable")
        
        self._detected_fields: Optional[Dict[str, Optional[str]]] = None
    
    async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
        """
        Stream scraped files one at a time for immediate processing.
        
        Yields:
            ScrapedFile objects as they are downloaded
        """
        try:
            # Authenticate if needed
            if self.config.get('authentication', {}).get('method') != 'none':
                await self.authenticate()
            
            session = await self._prepare_session()
            items = await self._list_items(session)
            
            # Detect field names from first item (MUST happen before filtering/duplicate detection)
            if items and not self._detected_fields:
                self._detect_api_fields(items[0])
            
            # Filter items by brand (before applying file limits)
            items = await self._filter_items_by_brand(items)
            
            # Track total files found after brand filtering (for duplicate detection reporting)
            self.total_files_found = len(items)
            
            # Get max_files limit (will check during iteration after duplicate detection)
            limits = self.config.get('limits', {})
            max_files = limits.get('max_files', None)
            if max_files is not None:
                logger.info(f"Max files per run: {max_files} (checking after duplicate detection, total available: {len(items)})")
            
            files_downloaded = 0
            for idx, item in enumerate(items, 1):
                # Stop if we've downloaded enough files (checked AFTER duplicate detection)
                if max_files is not None and files_downloaded >= max_files:
                    logger.info(f"Reached max files limit ({max_files} downloaded), stopping")
                    break
                
                # Skip already processed files if resuming from interruption
                if idx <= self.start_index:
                    logger.info(f"[RESUME] Skipping item {idx}/{len(items)} (resuming from index {self.start_index})")
                    continue
                
                # Check if file already processed (duplicate detection by supplier filename + date)
                if self.state_manager:
                    # Use auto-detected field names
                    brand_field = self._detected_fields.get('brand', 'Brand') if self._detected_fields else 'Brand'
                    valid_from_field = self._detected_fields.get('validFrom') if self._detected_fields else None
                    
                    api_brand = item.get(brand_field)
                    valid_from_str = item.get(valid_from_field) if valid_from_field else None
                    
                    # Generate the expected supplier filename pattern (what we'll download)
                    supplier_filename = self._generate_expected_filename(item)
                    
                    # Normalize the valid_from date to ISO format (for consistent comparison with state)
                    valid_from_iso = None
                    if valid_from_str:
                        valid_from_date = self._parse_date_string(valid_from_str)
                        if valid_from_date:
                            valid_from_iso = valid_from_date.isoformat()
                    
                    logger.info(
                        f"[DUPLICATE CHECK] Item {idx}/{len(items)}: supplier_filename={supplier_filename}, api_brand={api_brand}, valid_from={valid_from_iso}",
                        supplier_filename=supplier_filename,
                        api_brand=api_brand,
                        valid_from_date=valid_from_iso
                    )
                    
                    if supplier_filename and valid_from_iso:
                        # Check if this supplier filename+date combination was already processed
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename,
                            valid_from_date=valid_from_str
                        ):
                            logger.info(
                                f"[DUPLICATE SKIP] Item {idx}/{len(items)} - Already have: {supplier_filename} valid from {valid_from_str}. NOT downloading.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_str
                            )
                            self.files_skipped_duplicates += 1
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] Item {idx}/{len(items)} - Not in state: {supplier_filename} valid from {valid_from_str}. Will download.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_str
                            )
                
                files_downloaded += 1
                logger.info(f"[DOWNLOAD] Processing item {idx}/{len(items)} (file #{files_downloaded})")
                file = await self._export_item(session, item)
                if file:
                    logger.info(f"Successfully downloaded file: {file.filename}")
                    yield file  # Yield immediately for processing
        
        except Exception as e:
            logger.error(f"API client scraping failed: {e}")
            raise
        
        finally:
            # No need to close session here - it will be closed by context manager
            pass

    async def scrape(self) -> ScrapingResult:
        """
        Batch mode: Download all files first, then return them.
        
        This method is kept for backwards compatibility but scrape_stream() is preferred.
        """
        import time
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            # Authenticate if needed
            if self.config.get('authentication', {}).get('method') != 'none':
                await self.authenticate()
            
            session = await self._prepare_session()
            items = await self._list_items(session)
            
            # Filter items by brand (before applying file limits)
            items = await self._filter_items_by_brand(items)
            
            # Apply max_files limit if configured
            limits = self.config.get('limits', {})
            max_files = limits.get('max_files', None)
            if max_files is not None:
                logger.info(f"Limiting to {max_files} files (total available: {len(items)})")
                items = items[:max_files]
            
            files: List[ScrapedFile] = []
            
            for idx, item in enumerate(items, 1):
                logger.info(f"Processing item {idx}/{len(items)}")
                file = await self._export_item(session, item)
                if file:
                    files.append(file)
                    logger.info(f"Successfully saved file: {file.filename}")
            
            result.files = files
            result.success = len(files) > 0
        except Exception as e:
            logger.error(f"API client scraping failed: {str(e)}")
            result.errors.append(str(e))
        finally:
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    def _detect_api_fields(self, sample_item: Dict[str, Any]) -> None:
        """
        Use configured field names directly without validation.
        
        List metadata responses contain different fields than pricing data files,
        so we use the configured field names from brand_detection config instead
        of trying to detect pricing fields (partNumber, price) which don't exist
        in list metadata.
        
        Args:
            sample_item: Sample item from API response
        """
        if self._detected_fields is not None:
            return  # Already detected
        
        # Use field names from brand_detection config (these are optional metadata fields)
        brand_detection = self.config.get('brand_detection', {})
        self._detected_fields = {
            'brand': brand_detection.get('field', 'Brand'),
            'validFrom': 'ValidFrom',  # Use hardcoded defaults for metadata fields
            'validTo': 'ValidTo'
        }
        logger.info(f"Using metadata field names: {self._detected_fields}")
    
    async def _filter_items_by_brand(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter items by matching brands against brand_config.json and config array.
        
        Filtering steps:
        1. Extract brand from each item
        2. Filter by config array if configured
        3. Match against brand_config.json
        
        Args:
            items: List of items from API
            
        Returns:
            Filtered list containing only items with matching brands
        """
        brand_detection = self.config.get('brand_detection', {})
        
        # If no brand detection config, return all items
        if not brand_detection:
            logger.warning("No brand_detection config found - processing all items")
            return items
        
        # Use auto-detected brand field name, or fallback to config
        brand_field = self._detected_fields.get('brand') if self._detected_fields else brand_detection.get('field', 'Brand')
        pattern = brand_detection.get('pattern', '^([A-Z0-9_-]+)')
        
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
            logger.error("Failed to load brand configs - cannot filter items")
            return []
        
        filtered_items: List[Dict[str, Any]] = []
        skipped_brands: Dict[str, int] = {}
        matched_brands: Dict[str, int] = {}
        config_filtered_brands: Dict[str, int] = {}
        
        for item in items:
            # Extract brand from item
            brand_value = item.get(brand_field, '')
            if not brand_value:
                logger.warning(f"Item missing '{brand_field}' field: {item}")
                continue
            
            # Extract using pattern
            extracted_brand = extract_brand_from_text(str(brand_value), pattern)
            if not extracted_brand:
                logger.warning(f"Could not extract brand from '{brand_value}' using pattern '{pattern}'")
                continue
            
            # First filter: Check config array filter if configured
            if active_brands:
                if extracted_brand.upper() not in active_brands_upper:
                    # Brand not in config array, skip
                    config_filtered_brands[extracted_brand] = config_filtered_brands.get(extracted_brand, 0) + 1
                    continue
            
            # Second filter: Match against brand configs
            matched_config = find_matching_brand(extracted_brand, brand_configs)
            
            if matched_config:
                # Third filter: Check if we have parsing config for this brand
                # Get the config brand (e.g., BMW_FAST -> BMW_FAST)
                config_brand = matched_config.get('brand', extracted_brand)
                
                # Check if we can find parsing config (with parent fallback)
                scraper_configs_list = self.config.get('config', [])
                has_parsing_config = False
                search_brand = config_brand
                matched_parsing_brand = config_brand
                
                while search_brand:
                    for config in scraper_configs_list:
                        if config.get('brand', '').upper() == search_brand.upper():
                            has_parsing_config = True
                            matched_parsing_brand = search_brand
                            break
                    
                    if has_parsing_config:
                        break
                    
                    # Try parent brand
                    last_sep_pos = max(search_brand.rfind('_'), search_brand.rfind('-'))
                    if last_sep_pos > 0:
                        parent_brand = search_brand[:last_sep_pos]
                        logger.info(f"[PRE-DOWNLOAD CHECK] Config not found for {search_brand}, trying parent brand: {parent_brand}")
                        search_brand = parent_brand
                    else:
                        break
                
                if has_parsing_config:
                    # Only log if we had to use a parent brand's parsing config
                    if matched_parsing_brand != config_brand:
                        logger.info(f"[PRE-DOWNLOAD CHECK] ✓ Will use parent brand {matched_parsing_brand} parsing config for {config_brand}")
                    
                    # Fourth filter: Check if we have Drive folder ID for this brand (with parent fallback)
                    # Load brand configs to check for driveFolderId
                    brand_config_map = {b['brand'].upper(): b for b in brand_configs}
                    
                    has_drive_folder = False
                    drive_search_brand = config_brand
                    matched_drive_brand = config_brand
                    
                    # First try the config_brand, then matched_parsing_brand, then parent brands
                    search_brands = [config_brand]
                    if matched_parsing_brand != config_brand:
                        search_brands.append(matched_parsing_brand)
                    
                    for check_brand in search_brands:
                        test_brand = check_brand
                        while test_brand:
                            brand_cfg = brand_config_map.get(test_brand.upper())
                            if brand_cfg and brand_cfg.get('driveFolderId'):
                                has_drive_folder = True
                                matched_drive_brand = test_brand
                                drive_search_brand = test_brand
                                break
                            
                            # Try parent brand
                            last_sep_pos = max(test_brand.rfind('_'), test_brand.rfind('-'))
                            if last_sep_pos > 0:
                                test_brand = test_brand[:last_sep_pos]
                            else:
                                break
                        
                        if has_drive_folder:
                            break
                    
                    if has_drive_folder:
                        # Only log if we had to use a parent brand's Drive folder
                        if matched_drive_brand != config_brand:
                            logger.info(f"[PRE-DOWNLOAD CHECK] ✓ Will use parent brand {matched_drive_brand} Drive folder for {config_brand}")
                        
                        # Store the matched brand config in the item for later use
                        item['_matched_brand_config'] = matched_config
                        item['_extracted_brand'] = extracted_brand
                        filtered_items.append(item)
                        
                        # Track matched brands
                        brand_name = matched_config.get('brand', extracted_brand)
                        matched_brands[brand_name] = matched_brands.get(brand_name, 0) + 1
                    else:
                        # No Drive folder found (even after trying parent brands)
                        logger.warning(f"[PRE-DOWNLOAD CHECK] ✗ No Drive folder ID found for {config_brand} or {matched_parsing_brand} - SKIPPING download")
                        skipped_brands[f"{config_brand} (no Drive folder)"] = skipped_brands.get(f"{config_brand} (no Drive folder)", 0) + 1
                else:
                    # No parsing config found (even after trying parent brands)
                    logger.warning(f"[PRE-DOWNLOAD CHECK] ✗ No parsing config found for {config_brand} - SKIPPING download")
                    skipped_brands[f"{config_brand} (no parsing config)"] = skipped_brands.get(f"{config_brand} (no parsing config)", 0) + 1
            else:
                # Track skipped brands
                skipped_brands[extracted_brand] = skipped_brands.get(extracted_brand, 0) + 1
        
        # Log filtering results
        logger.info(f"Brand filtering: {len(filtered_items)}/{len(items)} items matched")
        
        if config_filtered_brands:
            filtered_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(config_filtered_brands.items())])
            logger.info(f"Filtered by config array: {filtered_summary}")
        
        if matched_brands:
            matched_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(matched_brands.items())])
            logger.info(f"Matched brands: {matched_summary}")
        
        if skipped_brands:
            skipped_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(skipped_brands.items())])
            logger.warning(f"Skipped brands (not in brand_config.json): {skipped_summary}")
        
        return filtered_items
    
    async def authenticate(self) -> bool:
        """Authenticate and harvest session cookies/tokens."""
        auth_config = self.config.get('authentication', {})
        auth_method = auth_config.get('method', 'none')
        
        if auth_method == 'none':
            return True
        
        try:
            if auth_method == 'form':
                return await self._authenticate_form(auth_config)
            elif auth_method == 'bearer':
                return await self._authenticate_bearer(auth_config)
            else:
                logger.warning(f"Unsupported auth method for API client: {auth_method}")
                return False
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    async def _authenticate_form(self, auth_config: Dict) -> bool:
        """Perform form-based authentication and harvest cookies."""
        page = None
        try:
            credentials = self.credential_manager.get_credentials()
            
            username = credentials.username
            password = credentials.password
            
            if not username or not password:
                logger.error(f"No credentials found for {self.supplier_name}")
                return False
            
            # Use browser to login and harvest cookies
            page = self.browser_manager._ensure_page_initialized()
            
            # Navigate to login page
            login_url = auth_config['login_url']
            logger.info(f"Navigating to login page: {login_url}")
            await page.goto(login_url, timeout=30000)
            await page.wait_for_load_state('domcontentloaded', timeout=10000)
            
            # Take screenshot before login
            await self.browser_manager.take_screenshot("01_login_page")
            
            # Fill login form
            username_field = auth_config['username_field']
            password_field = auth_config['password_field']
            submit_button = auth_config['submit_button']
            
            logger.info(f"Filling login form with username: {username}")
            await page.fill(username_field, username)
            await page.fill(password_field, password)
            
            # Take screenshot before submitting
            await self.browser_manager.take_screenshot("02_before_submit")
            
            await page.click(submit_button)
            logger.info("Login form submitted, waiting for response...")
            
            # Wait for navigation or success indicator
            success_indicator = auth_config.get('success_indicator', None)
            if success_indicator:
                # Wait for a specific element that indicates successful login
                await page.wait_for_selector(success_indicator, timeout=15000)
            else:
                # Wait for URL to change or a reasonable timeout
                try:
                    await page.wait_for_url(lambda url: url != login_url, timeout=15000)
                except Exception:
                    # URL didn't change, but that might be ok - check for cookies anyway
                    await page.wait_for_timeout(3000)
            
            # Take screenshot after login
            await self.browser_manager.take_screenshot("03_after_login")
            
            # Check current URL to verify login success
            current_url = page.url
            logger.info(f"Current URL after login: {current_url}")
            
            # Harvest cookies for API requests
            cookies = await page.context.cookies()
            logger.info(f"Total cookies found: {len(cookies)}")
            for cookie in cookies:
                logger.info(f"Cookie: {cookie.get('name', 'unknown')} = {cookie.get('value', '')[:20]}... (domain: {cookie.get('domain', 'unknown')})")
            
            self.session_cookies = {cookie.get('name', ''): cookie.get('value', '') for cookie in cookies if cookie.get('name')}
            
            # Check for token storage configuration
            token_config = auth_config.get('token_storage', None)
            bearer_token = None
            
            if token_config:
                # Config-driven token extraction
                bearer_token = await self._extract_token_from_storage(page, token_config)
            else:
                # Legacy: log storage contents for debugging
                try:
                    local_storage = await page.evaluate("() => JSON.stringify(localStorage)")
                    session_storage = await page.evaluate("() => JSON.stringify(sessionStorage)")
                    logger.info(f"LocalStorage: {local_storage[:200]}")
                    logger.info(f"SessionStorage: {session_storage[:200]}")
                except Exception as e:
                    logger.warning(f"Could not read storage: {e}")
            
            # Check if we have either cookies or bearer token
            if not self.session_cookies and not bearer_token:
                logger.warning(f"No cookies or bearer token found for {self.supplier_name}")
                logger.warning(f"This might indicate: 1) Login failed, 2) Unknown auth method")
                return False
            
            auth_type = "bearer token" if bearer_token else f"{len(self.session_cookies)} cookies"
            logger.info(f"Authentication successful for {self.supplier_name}, using {auth_type}")
            return True
            
        except Exception as e:
            logger.error(f"Form authentication failed: {e}")
            if page:
                try:
                    await self.browser_manager.take_screenshot("error_auth_failed")
                except Exception:
                    pass
            return False
    
    async def _extract_token_from_storage(self, page, token_config: Dict[str, Any]) -> Optional[str]:
        """
        Extract authentication token from browser storage based on config.
        
        Args:
            page: Playwright page object
            token_config: Configuration dict with:
                - type: "localStorage" or "sessionStorage"
                - path: Dot-notation path to token (e.g., "loginData.Token")
                - header_name: Name of header to set (e.g., "Authorization")
                - header_format: Format string with {token} placeholder (e.g., "Bearer {token}")
        
        Returns:
            The extracted token, or None if not found
        """
        import json
        
        try:
            storage_type = token_config.get('type', 'localStorage')
            token_path = token_config.get('path', '')
            header_name = token_config.get('header_name', 'Authorization')
            header_format = token_config.get('header_format', 'Bearer {token}')
            
            # Get storage data
            if storage_type == 'localStorage':
                storage_json = await page.evaluate("() => JSON.stringify(localStorage)")
            elif storage_type == 'sessionStorage':
                storage_json = await page.evaluate("() => JSON.stringify(sessionStorage)")
            else:
                logger.error(f"Unknown storage type: {storage_type}")
                return None
            
            logger.info(f"Reading {storage_type}: {storage_json[:200]}")
            storage_data = json.loads(storage_json)
            
            # Navigate the path to find the token
            path_parts = token_path.split('.')
            current_value: Any = storage_data
            
            for part in path_parts:
                if isinstance(current_value, str):
                    # Need to parse nested JSON
                    current_value = json.loads(current_value)
                
                if isinstance(current_value, dict) and part in current_value:
                    current_value = current_value[part]
                else:
                    logger.warning(f"Could not find '{part}' in path '{token_path}'")
                    return None
            
            token = str(current_value)
            logger.info(f"Found token at {storage_type}.{token_path}: {token[:50]}...")
            
            # Format and set the header
            header_value = header_format.replace('{token}', token)
            self.session_headers[header_name] = header_value
            logger.info(f"Set header {header_name}: {header_value[:60]}...")
            
            return token
            
        except Exception as e:
            logger.error(f"Failed to extract token from {token_config.get('type', 'storage')}: {e}")
            return None
    
    async def _authenticate_bearer(self, auth_config: Dict) -> bool:
        """Set up bearer token authentication."""
        try:
            # Note: Bearer tokens are typically stored as passwords in credential manager
            # The "username" field can be used for token name/identifier
            credentials = self.credential_manager.get_credentials()
            
            # Use password field as the bearer token
            token_value = credentials.password
            
            if not token_value:
                logger.error(f"No bearer token found for {self.supplier_name}")
                return False
            
            token_header = auth_config.get('token_header', 'Authorization')
            
            if not token_value.startswith('Bearer '):
                token_value = f"Bearer {token_value}"
            
            self.session_headers[token_header] = token_value
            
            logger.info(f"Bearer token authentication set up for {self.supplier_name}")
            return True
            
        except Exception as e:
            logger.error(f"Bearer authentication failed: {e}")
            return False

    async def navigate_to_downloads(self) -> bool:
        return True

    async def download_files(self) -> List[ScrapedFile]:
        return []

    async def _prepare_session(self) -> httpx.Client:
        """Create an httpx session with authentication."""
        api_cfg = self.config.get('api', {})
        base_url = api_cfg.get('base_url', '')
        
        # Prepare headers
        headers = api_cfg.get('headers', {}).copy()
        headers.update(self.session_headers)
        
        # Prepare cookies
        cookies = self.session_cookies.copy()
        
        # Create client with base URL, headers, and cookies
        client = httpx.Client(
            base_url=base_url,
            headers=headers,
            cookies=cookies,
            timeout=60.0,
            follow_redirects=True
        )
        
        return client

    async def _list_items(self, client: httpx.Client) -> List[Dict[str, Any]]:
        """List available items from the API."""
        api_cfg = self.config.get('api', {})
        list_endpoint = api_cfg['list_endpoint']
        method = api_cfg.get('list_method', 'GET').upper()
        params = api_cfg.get('list_params', {})
        
        logger.info(f"Listing items from {list_endpoint}")
        
        if method == 'GET':
            response = client.get(list_endpoint, params=params)
        else:
            response = client.request(method, list_endpoint, json=params)
        
        response.raise_for_status()
        data = response.json()
        
        # Extract items using the configured path
        items_path = api_cfg.get('list_items_path', 'data')
        items: Any = data
        for key in items_path.split('.'):
            items = items.get(key, [])
        
        if not isinstance(items, list):
            items = [items]
        
        logger.info(f"Found {len(items)} items")
        
        # DEBUG: Log sample item structure to identify available fields
        if items:
            import json
            sample_item = items[0]
            logger.info(f"[DEBUG] Sample API item structure: {json.dumps(sample_item, indent=2, default=str)}")
            logger.info(f"[DEBUG] Available fields in API response: {list(sample_item.keys())}")
        
        return cast(List[Dict[str, Any]], items)

    async def _export_item(self, client: httpx.Client, item: Dict[str, Any]) -> Optional[ScrapedFile]:
        """Export a single item to a file."""
        api_cfg = self.config.get('api', {})
        export_endpoint = api_cfg['export_endpoint']
        export_method = api_cfg.get('export_method', 'GET').upper()
        params_tmpl = api_cfg.get('export_params_template', {})
        
        # Build params from template and item fields
        params = {}
        for k, v in params_tmpl.items():
            if isinstance(v, str) and v.startswith('{') and v.endswith('}'):
                key = v[1:-1]
                params[k] = item.get(key)
            else:
                params[k] = v
        
        logger.info(f"Exporting item with params: {params}")
        
        # Add Accept header to request Excel format
        export_headers = {
            'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel, application/json, */*'
        }
        
        if export_method == 'GET':
            response = client.get(export_endpoint, params=params, headers=export_headers)
        else:
            response = client.request(export_method, export_endpoint, json=params, headers=export_headers)
        
        response.raise_for_status()
        
        # Log response details for debugging
        content_type = response.headers.get('Content-Type', 'unknown')
        content_length = response.headers.get('Content-Length', 'unknown')
        logger.info(f"Response Content-Type: {content_type}, Content-Length: {content_length}")
        logger.info(f"All response headers: {dict(response.headers)}")
        
        # Generate filename
        filename = self._derive_filename(item, response)
        
        # Use configured download directory if available, otherwise use temp
        if hasattr(self.browser_manager, 'download_dir') and self.browser_manager.download_dir:
            download_dir = self.browser_manager.download_dir
            os.makedirs(download_dir, exist_ok=True)
        else:
            download_dir = tempfile.mkdtemp(prefix=f"api_{self.supplier_name}_")
        
        # Check if response is JSON - save directly (no Excel conversion for performance)
        if content_type.startswith('application/json'):
            # Save JSON directly for fast streaming parsing (with .xlsx extension for duplicate detection)
            import json
            try:
                json_data = response.json()
                
                # Extract data array (handle different response structures)
                if isinstance(json_data, dict) and 'Data' in json_data:
                    data = json_data['Data']
                elif isinstance(json_data, list):
                    data = json_data
                else:
                    data = [json_data]
                
                # Save as JSON with .xlsx extension (parser will detect actual content)
                # Using .xlsx extension maintains consistent duplicate detection across runs
                file_path = os.path.join(download_dir, filename)  # filename already has .xlsx from _derive_filename
                
                logger.info(f"Saving JSON data (as .xlsx): {file_path} ({len(data)} rows)")
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f)
                
            except Exception as e:
                logger.warning(f"Failed to save JSON: {e}, saving raw response")
                file_path = os.path.join(download_dir, filename)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
        else:
            # Save binary content as-is (Excel, CSV, etc.)
            file_path = os.path.join(download_dir, filename)
            logger.info(f"Saving file to: {file_path}")
            with open(file_path, 'wb') as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
        
        # For web scrapers: preserve supplier's original brand categorization
        # (e.g., "BMW_PART1", "BMW_PART2" instead of normalized "BMW")
        # This is critical for filename generation as per design spec
        # Use auto-detected field names
        brand_field = self._detected_fields.get('brand', 'Brand') if self._detected_fields else 'Brand'
        raw_supplier_brand = item.get(brand_field)  # Keep original: "BMW_PART1", "VAG-OIL"
        
        # Extract dates from item if configured (keep as raw strings from supplier)
        scraped_file_kwargs = {'brand': raw_supplier_brand}  # Use 'brand' for backward compatibility
        
        # Extract valid_from date if field is configured (no parsing, keep raw string)
        valid_from_field = self._detected_fields.get('validFrom') if self._detected_fields else None
        if valid_from_field and valid_from_field in item:
            valid_from_str = item.get(valid_from_field)
            if valid_from_str:
                scraped_file_kwargs['valid_from_date_str'] = valid_from_str
                logger.info(f"Extracted valid_from date: {valid_from_str}")
        
        # Extract valid_to/expiry date if field is configured (no parsing, keep raw string)
        valid_to_field = self._detected_fields.get('validTo') if self._detected_fields else None
        if valid_to_field and valid_to_field in item:
            valid_to_str = item.get(valid_to_field)
            if valid_to_str:
                scraped_file_kwargs['expiry_date_str'] = valid_to_str
                logger.info(f"Extracted valid_to date: {valid_to_str}")
        
        return self.create_scraped_file(
            filename=filename,
            local_path=file_path,
            **scraped_file_kwargs
        )

    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """
        Parse date string to datetime object.
        
        Handles common date formats:
        - ISO8601: 2025-10-26T16:10:31Z
        - Date only: 2025-10-26
        - DD/MM/YYYY: 26/10/2025
        - DD-MM-YYYY: 26-10-2025
        
        Args:
            date_str: Date string to parse
            
        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not date_str:
            return None
        
        from dateutil import parser as date_parser
        
        try:
            # Use dateutil.parser for flexible parsing
            dt = date_parser.parse(date_str)
            
            # Ensure timezone aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            return dt
        except Exception as e:
            logger.debug(f"Failed to parse date string '{date_str}': {e}")
            return None
    
    def _generate_expected_filename(self, item: Dict[str, Any]) -> str:
        """
        Generate deterministic supplier filename for duplicate detection.
        
        MUST match the format used by _derive_filename().
        
        Format: {Brand}_{Supplier}_{Location}_{Currency}_{MMYY}.xlsx
        Uses date from ValidFrom (not current time) for determinism.
        
        Args:
            item: Item from API with Brand and ValidFrom
            
        Returns:
            Supplier filename, e.g., "BMW_PART1_NEOPARTA_LITHUANIA_EUR_1024.xlsx"
        """
        brand_value = item.get('Brand', 'UNKNOWN')
        normalized_supplier_filename = normalize_brand(brand_value)
        location = self.config.get('location', '')
        currency = self.config.get('currency', '')
        
        parts = [normalized_supplier_filename, self.supplier_name]
        if location:
            parts.append(location)
        if currency:
            parts.append(currency)
        
        # Use date from ValidFrom field for deterministic naming
        valid_from_str = item.get('ValidFrom', '')
        if valid_from_str:
            try:
                dt = self._parse_date_string(valid_from_str)
                if dt:
                    date_part = dt.strftime('%m%y')
                    parts.append(date_part)
                    return f"{'_'.join(parts)}.xlsx"
            except Exception as e:
                logger.warning(f"Could not parse ValidFrom '{valid_from_str}': {e}")
        
        # Fallback: use current month (old behavior)
        logger.warning(f"No ValidFrom for {brand_value}, using current month")
        parts.append(datetime.now().strftime('%m%y'))
        return f"{'_'.join(parts)}.xlsx"
    
    def _derive_filename(self, item: Dict[str, Any], response: httpx.Response) -> str:
        """
        Derive supplier filename from response or item data.
        
        MUST match the format used by _generate_expected_filename().
        
        Format: {Brand}_{Supplier}_{Location}_{Currency}_{MMYY}.{ext}
        Uses date from ValidFrom (not current time) for determinism.
        """
        # Try Content-Disposition header first
        cd = response.headers.get('Content-Disposition')
        if cd:
            match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', cd)
            if match:
                filename = match.group(1).strip('\'"')
                if filename:
                    logger.debug(f"Using filename from Content-Disposition: {filename}")
                    return filename
        
        # Determine extension from Content-Type
        # Note: We use .xlsx extension even for JSON to maintain consistent duplicate detection
        content_type = response.headers.get('Content-Type', '').lower()
        if 'json' in content_type:
            extension = 'xlsx'  # Use .xlsx extension for duplicate detection consistency
        elif 'excel' in content_type or 'spreadsheet' in content_type:
            extension = 'xlsx'
        elif 'csv' in content_type:
            extension = 'csv'
        else:
            # Default to xlsx for API responses
            extension = 'xlsx'
        
        # Generate deterministic filename
        brand_value = item.get('Brand', 'UNKNOWN')
        normalized_supplier_filename = normalize_brand(brand_value)
        location = self.config.get('location', '')
        currency = self.config.get('currency', '')
        
        parts = [normalized_supplier_filename, self.supplier_name]
        if location:
            parts.append(location)
        if currency:
            parts.append(currency)
        
        # Use date from ValidFrom field for deterministic naming
        valid_from_str = item.get('ValidFrom', '')
        if valid_from_str:
            try:
                dt = self._parse_date_string(valid_from_str)
                if dt:
                    date_part = dt.strftime('%m%y')
                    parts.append(date_part)
                    filename = f"{'_'.join(parts)}.{extension}"
                    logger.debug(f"Generated deterministic filename: {filename}")
                    return filename
            except Exception as e:
                logger.warning(f"Could not parse ValidFrom '{valid_from_str}': {e}")
        
        # Fallback: use current month (should rarely happen)
        logger.warning(f"Using current month fallback for {brand_value}")
        parts.append(datetime.now().strftime('%m%y'))
        return f"{'_'.join(parts)}.{extension}"

