"""
APF Scraper - Pure REST API client for Wiuse pricing API.

This scraper connects to the Wiuse pricing API to download price lists.
It does NOT require a browser - authentication is via a static API key.

API Documentation: https://pricing.wiuse.net/swagger/index.html

Endpoints:
- GET /pricelists - Lists all available price lists with version info
- GET /download-pricelist?brandCode=XX - Downloads a price list as file stream
"""

import os
import re
import tempfile
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, AsyncIterator, cast
from pathlib import Path

import httpx

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import (
    extract_brand_from_text,
    normalize_brand,
    find_matching_brand,
    load_brand_configs,
)
from parsers.field_name_detector import FieldNameDetector
from utils.logger import get_logger

logger = get_logger(__name__)


class ApfScraper(BaseScraper):
    """
    Pure REST API scraper for APF/Wiuse pricing API.
    
    Features:
    - Bearer token authentication from environment variable
    - List available price lists via /pricelists endpoint
    - Download files via /download-pricelist endpoint
    - Version-based duplicate detection (version field, fallback to date)
    - Brand alias resolution (API codes like "HY" -> "HYUNDAI")
    
    This scraper does NOT use Playwright/browser - it's a pure HTTP client.
    """
    
    def __init__(
        self,
        scraper_config: Dict[str, Any],
        browser_manager: Any,
        start_index: int = 0,
        state_manager: Optional[Any] = None,
    ):
        """
        Initialize the APF scraper.
        
        Args:
            scraper_config: Supplier-specific scraping configuration
            browser_manager: BrowserManager instance (not used, kept for interface)
            start_index: Index to resume from (for interrupted runs)
            state_manager: StateManager for duplicate detection
        """
        super().__init__(scraper_config, browser_manager, start_index, state_manager)
        
        # HTTP client state
        self._client: Optional[httpx.Client] = None
        self._api_key: Optional[str] = None
        
        # Download directory (use browser_manager's if available)
        if hasattr(browser_manager, 'download_dir') and browser_manager.download_dir:
            self.download_dir = browser_manager.download_dir
        else:
            self.download_dir = f"./.scraper_downloads/{self.supplier_name}"
        
        # Metrics for duplicate detection reporting
        self.total_files_found: int = 0
        self.files_skipped_duplicates: int = 0
        
        # Initialize field name detector for API response field detection
        column_mapping_path = Path("config/core/column_mapping_config.json")
        if column_mapping_path.exists():
            with open(column_mapping_path, 'r') as f:
                column_mapping_config = json.load(f)
            self.field_name_detector = FieldNameDetector(column_mapping_config)
        else:
            logger.warning("column_mapping_config.json not found - field detection disabled")
            self.field_name_detector = None
        
        # Cached field mappings (detected from first API response)
        self._detected_fields: Optional[Dict[str, Optional[str]]] = None
    
    async def authenticate(self) -> bool:
        """
        Set up Bearer token authentication from environment variable.
        
        The API key is read from the environment variable specified in
        config.authentication.password_env (default: SCRAPER_APF_API_KEY).
        
        Returns:
            True if API key was successfully loaded, False otherwise
        """
        auth_config = self.config.get('authentication', {})
        password_env = auth_config.get('password_env', 'SCRAPER_APF_API_KEY')
        
        self._api_key = os.getenv(password_env)
        
        if not self._api_key:
            logger.error(
                f"No API key found for {self.supplier_name}. "
                f"Please set environment variable: {password_env}"
            )
            return False
        
        # Log truncated key for debugging
        key_preview = self._api_key[:20] + "..." if len(self._api_key) > 20 else self._api_key
        logger.info(f"API key loaded from {password_env} for {self.supplier_name}: {key_preview}")
        return True
    
    async def navigate_to_downloads(self) -> bool:
        """Not applicable for REST API - always returns True."""
        return True
    
    async def download_files(self) -> List[ScrapedFile]:
        """
        Not used - scrape_stream() is the primary interface.
        
        Returns:
            Empty list (use scrape_stream() instead)
        """
        return []
    
    async def scrape(self) -> ScrapingResult:
        """
        Batch mode scraping - downloads all files first, then returns them.
        
        This method is kept for backwards compatibility but scrape_stream() 
        is preferred for better memory usage and immediate processing.
        
        Returns:
            ScrapingResult containing all downloaded files
        """
        import time
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            files: List[ScrapedFile] = []
            async for file in self.scrape_stream():
                files.append(file)
            
            result.files = files
            result.success = len(files) > 0
            result.total_files_found = self.total_files_found
            result.files_skipped_duplicates = self.files_skipped_duplicates
            
        except Exception as e:
            logger.error(f"APF scraping failed: {e}")
            result.errors.append(str(e))
        finally:
            result.execution_time_seconds = time.time() - start_time
            self._close_client()
        
        return result
    
    async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
        """
        Stream scraped files one at a time for immediate processing.
        
        This is the primary scraping interface. Files are yielded as they
        are downloaded, allowing for immediate processing without loading
        all files into memory.
        
        Yields:
            ScrapedFile objects as they are downloaded
        """
        try:
            # Authenticate (load API key)
            if not await self.authenticate():
                logger.error("Authentication failed - cannot proceed")
                return
            
            # Create HTTP client
            client = self._get_client()
            
            # List available price lists
            items = await self._list_items(client)
            
            # Filter by enabled brands (with alias resolution)
            items = await self._filter_items_by_brand(items)
            
            # Track total files after brand filtering
            self.total_files_found = len(items)
            
            # Apply max_files limit
            limits = self.config.get('limits', {})
            max_files = limits.get('max_files')
            if max_files is not None:
                logger.info(
                    f"Max files per run: {max_files} "
                    f"(checking after duplicate detection, total available: {len(items)})"
                )
            
            files_downloaded = 0
            
            for idx, item in enumerate(items, 1):
                # Stop if we've hit the max files limit
                if max_files is not None and files_downloaded >= max_files:
                    logger.info(f"Reached max files limit ({max_files} downloaded), stopping")
                    break
                
                # Skip already processed files (resumption support)
                if idx <= self.start_index:
                    logger.info(
                        f"[RESUME] Skipping item {idx}/{len(items)} "
                        f"(resuming from index {self.start_index})"
                    )
                    continue
                
                # Duplicate detection
                if self._should_skip_item(item, idx, len(items)):
                    self.files_skipped_duplicates += 1
                    continue
                
                # Download the file
                files_downloaded += 1
                logger.info(f"[DOWNLOAD] Processing item {idx}/{len(items)} (file #{files_downloaded})")
                
                file = await self._download_item(client, item)
                if file:
                    logger.info(f"Successfully downloaded file: {file.filename}")
                    yield file
        
        except Exception as e:
            logger.error(f"APF scraping failed: {e}")
            raise
        finally:
            self._close_client()
    
    def _get_client(self) -> httpx.Client:
        """
        Get or create the HTTP client with authentication.
        
        Returns:
            Configured httpx.Client instance
        """
        if self._client is None:
            api_cfg = self.config.get('api', {})
            base_url = api_cfg.get('base_url', 'https://pricing.wiuse.net')
            
            # Prepare headers with Bearer token
            headers: Dict[str, str] = {}
            if self._api_key:
                token = self._api_key
                if not token.startswith('Bearer '):
                    token = f"Bearer {token}"
                headers['Authorization'] = token
            
            self._client = httpx.Client(
                base_url=base_url,
                headers=headers,
                timeout=60.0,
                follow_redirects=True,
            )
        
        return self._client
    
    def _close_client(self) -> None:
        """Close the HTTP client if open."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing HTTP client: {e}")
            self._client = None
    
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
        
        # Use field names from brand_detection config
        brand_detection = self.config.get('brand_detection', {})
        self._detected_fields = {
            'brand': brand_detection.get('field', 'brandCode'),
            'version': 'version',
            'createdDate': 'createdDateTime',
            'isNew': 'isNew'
        }
        logger.info(f"Using metadata field names: {self._detected_fields}")
    
    async def _list_items(self, client: httpx.Client) -> List[Dict[str, Any]]:
        """
        List available price lists from the API.
        
        Calls GET /pricelists and returns the priceLists array.
        
        Args:
            client: HTTP client instance
            
        Returns:
            List of price list items from the API
        """
        api_cfg = self.config.get('api', {})
        list_endpoint = api_cfg.get('list_endpoint', '/pricelists')
        list_items_path = api_cfg.get('list_items_path', 'priceLists')
        
        logger.info(f"Listing items from {list_endpoint}")
        
        response = client.get(list_endpoint)
        response.raise_for_status()
        
        data = response.json()
        
        # Navigate to items using configured path
        items: Any = data
        for key in list_items_path.split('.'):
            items = items.get(key, [])
        
        if not isinstance(items, list):
            items = [items]
        
        # Don't detect fields on list metadata - use configured field names from brand_detection config
        # Field detection is only needed for downloaded files with pricing data
        
        logger.info(f"Found {len(items)} items")
        return cast(List[Dict[str, Any]], items)
    
    async def _filter_items_by_brand(
        self, items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Filter items by matching brands against brand_config.json and config array.
        
        Filtering steps:
        1. Extract brand code from each item (e.g., "HY", "KI")
        2. Resolve via aliases in brand_config.json (HY -> HYUNDAI)
        3. Check against config array filter
        4. Verify parsing config and Drive folder exist
        
        Args:
            items: List of items from API
            
        Returns:
            Filtered list of items with _matched_brand_config attached
        """
        brand_detection = self.config.get('brand_detection', {})
        
        if not brand_detection:
            logger.warning("No brand_detection config - processing all items")
            return items
        
        brand_field = brand_detection.get('field', 'brandCode')
        pattern = brand_detection.get('pattern', '^([A-Z0-9_-]+)')
        
        # Extract active brands from config array (single source of truth)
        config_brands = self.config.get('config', [])
        active_brands = [
            cfg['brand'] 
            for cfg in config_brands 
            if cfg.get('enabled', True)  # enabled by default
        ]
        active_brands_upper = [b.upper() for b in active_brands]
        
        if active_brands:
            logger.info(f"Filtering by config brands: {', '.join(active_brands)}")
        
        # Load brand configs (uses cache set by orchestrator)
        brand_configs = load_brand_configs()
        if not brand_configs:
            logger.error("Failed to load brand configs - cannot filter items")
            return []
        
        brand_config_map = {cfg['brand'].upper(): cfg for cfg in brand_configs}
        
        filtered_items: List[Dict[str, Any]] = []
        skipped_brands: Dict[str, int] = {}
        matched_brands: Dict[str, int] = {}
        config_filtered_brands: Dict[str, int] = {}
        
        for item in items:
            brand_value = item.get(brand_field, '')
            if not brand_value:
                logger.warning(f"Item missing '{brand_field}' field: {item}")
                continue
            
            # Extract brand using pattern
            extracted_brand = extract_brand_from_text(str(brand_value), pattern)
            if not extracted_brand:
                logger.warning(
                    f"Could not extract brand from '{brand_value}' "
                    f"using pattern '{pattern}'"
                )
                continue
            
            # Check supplier-level brand aliases first (takes precedence over global aliases)
            supplier_aliases = self.config.get('brand_aliases', {})
            canonical_brand: Optional[str] = None
            
            if supplier_aliases:
                # Create case-insensitive lookup
                aliases_lower = {k.lower(): v for k, v in supplier_aliases.items()}
                if extracted_brand.lower() in aliases_lower:
                    # Direct mapping from supplier alias to canonical brand
                    canonical_brand = aliases_lower[extracted_brand.lower()]
                    logger.debug(
                        f"Resolved '{extracted_brand}' to '{canonical_brand}' "
                        f"via supplier-level alias"
                    )
                    # Fix lint error #1: Check if canonical_brand is not None
                    if canonical_brand:
                        matched_config = find_matching_brand(canonical_brand, brand_configs)
                else:
                    # Fallback to global brand_config.json aliases
                    matched_config = find_matching_brand(extracted_brand, brand_configs)
            else:
                # No supplier aliases defined, use global aliases
                matched_config = find_matching_brand(extracted_brand, brand_configs)
            
            if not matched_config:
                skipped_brands[extracted_brand] = skipped_brands.get(extracted_brand, 0) + 1
                continue
            
            # Get canonical brand name
            config_brand = matched_config.get('brand', extracted_brand)
            
            # Check config array filter (using canonical name)
            if active_brands_upper and config_brand.upper() not in active_brands_upper:
                config_filtered_brands[config_brand] = config_filtered_brands.get(config_brand, 0) + 1
                continue
            
            # Verify we have parsing config for this brand
            scraper_configs = self.config.get('config', [])
            has_parsing_config = self._find_parsing_config(config_brand, scraper_configs)
            
            if not has_parsing_config:
                skipped_brands[f"{config_brand} (no parsing config)"] = (
                    skipped_brands.get(f"{config_brand} (no parsing config)", 0) + 1
                )
                continue
            
            # Verify we have Drive folder for this brand
            has_drive_folder = self._has_drive_folder(config_brand, brand_config_map)
            
            if not has_drive_folder:
                skipped_brands[f"{config_brand} (no Drive folder)"] = (
                    skipped_brands.get(f"{config_brand} (no Drive folder)", 0) + 1
                )
                continue
            
            # All checks passed - include this item
            item['_matched_brand_config'] = matched_config
            item['_extracted_brand'] = extracted_brand
            item['_canonical_brand'] = config_brand
            filtered_items.append(item)
            
            matched_brands[config_brand] = matched_brands.get(config_brand, 0) + 1
        
        # Log filtering summary
        logger.info(f"Brand filtering: {len(filtered_items)}/{len(items)} items matched")
        
        if config_filtered_brands:
            summary = ", ".join(
                f"{b} ({c})" for b, c in sorted(config_filtered_brands.items())
            )
            logger.info(f"Filtered by config array: {summary}")
        
        if matched_brands:
            summary = ", ".join(
                f"{b} ({c})" for b, c in sorted(matched_brands.items())
            )
            logger.info(f"Matched brands: {summary}")
        
        if skipped_brands:
            summary = ", ".join(
                f"{b} ({c})" for b, c in sorted(skipped_brands.items())
            )
            logger.warning(f"Skipped brands: {summary}")
        
        return filtered_items
    
    def _find_parsing_config(
        self, brand: str, scraper_configs: List[Dict[str, Any]]
    ) -> bool:
        """
        Check if we have parsing config for this brand (with parent fallback).
        
        Args:
            brand: Brand name to check
            scraper_configs: List of brand configs from scraper config
            
        Returns:
            True if parsing config exists for this brand or a parent brand
        """
        search_brand = brand
        
        while search_brand:
            for config in scraper_configs:
                if config.get('brand', '').upper() == search_brand.upper():
                    return True
            
            # Try parent brand (e.g., BMW_PART1 -> BMW)
            last_sep = max(search_brand.rfind('_'), search_brand.rfind('-'))
            if last_sep > 0:
                search_brand = search_brand[:last_sep]
            else:
                break
        
        return False
    
    def _has_drive_folder(
        self, brand: str, brand_config_map: Dict[str, Dict[str, Any]]
    ) -> bool:
        """
        Check if this brand has a Drive folder ID configured (with parent fallback).
        
        Args:
            brand: Brand name to check
            brand_config_map: Map of brand names to brand configs
            
        Returns:
            True if Drive folder ID exists for this brand or a parent brand
        """
        search_brand = brand
        
        while search_brand:
            cfg = brand_config_map.get(search_brand.upper())
            if cfg and cfg.get('driveFolderId'):
                return True
            
            # Try parent brand
            last_sep = max(search_brand.rfind('_'), search_brand.rfind('-'))
            if last_sep > 0:
                search_brand = search_brand[:last_sep]
            else:
                break
        
        return False
    
    def _should_skip_item(
        self, item: Dict[str, Any], idx: int, total: int
    ) -> bool:
        """
        Check if item should be skipped due to duplicate detection.
        
        Uses our own state management for version/date comparison:
        1. Version comparison - skip if same version already processed
        2. Date comparison - skip if same date already processed
        
        Note: We intentionally ignore the API's isNew flag to maintain
        full control over duplicate detection.
        
        Args:
            item: Item from API
            idx: Current item index (1-based)
            total: Total number of items
            
        Returns:
            True if item should be skipped, False if it should be downloaded
        """
        if not self.state_manager:
            return False
        
        # Use auto-detected field names (or fallback to defaults)
        if not self._detected_fields:
            self._detect_api_fields(item)
        
        # Fix lint errors #2-4: Handle case where self._detected_fields could be None
        brand_field = self._detected_fields.get('brand', 'brandCode') if self._detected_fields else 'brandCode'
        version_field = self._detected_fields.get('version') if self._detected_fields else None
        created_date_field = self._detected_fields.get('createdDate') if self._detected_fields else None
        # Note: We intentionally don't use isNew field - we have our own state management
        
        api_brand = item.get(brand_field) if brand_field else None
        version_str = item.get(version_field) if version_field else None
        date_str = item.get(created_date_field) if created_date_field else None
        
        # Note: We intentionally ignore the API's isNew flag and use our own state management
        # This ensures we have full control over duplicate detection
        
        # Generate expected filename for state lookup
        supplier_filename = self._generate_expected_filename(item)
        
        # Determine comparison key - ALWAYS use date for consistency with state storage
        # (version changes too frequently and doesn't match what we store)
        comparison_key = date_str
        if comparison_key:
            # Normalize date for comparison (ensure ISO format)
            # Fix lint error #5: Check if date_str is not None before parsing
            parsed_date = self._parse_date_string(date_str) if date_str else None
            if parsed_date:
                comparison_key = date_str  # Keep original format for exact match
        
        logger.info(
            f"[DUPLICATE CHECK] Item {idx}/{total}: "
            f"supplier_filename={supplier_filename}, api_brand={api_brand}, "
            f"version={version_str}, date={date_str}",
            supplier_filename=supplier_filename,
            api_brand=api_brand,
            version=version_str,
            valid_from_date=date_str,
        )
        
        if supplier_filename and comparison_key:
            if self.state_manager.is_file_already_processed(
                supplier=self.supplier_name,
                supplier_filename=supplier_filename,
                valid_from_date=comparison_key,
            ):
                logger.info(
                    f"[DUPLICATE SKIP] Item {idx}/{total} - Already have: "
                    f"{supplier_filename} version/date={comparison_key}. NOT downloading.",
                    supplier_filename=supplier_filename,
                    version=version_str,
                    valid_from_date=date_str,
                )
                return True
            else:
                logger.info(
                    f"[NEW FILE] Item {idx}/{total} - Not in state: "
                    f"{supplier_filename} version/date={comparison_key}. Will download.",
                    supplier_filename=supplier_filename,
                    version=version_str,
                    valid_from_date=date_str,
                )
        
        return False
    
    def _generate_expected_filename(self, item: Dict[str, Any]) -> str:
        """
        Generate deterministic supplier filename for duplicate detection.
        
        MUST match the format used by _extract_filename_from_response().
        
        Format: {brandCode}_{YYYYMMDD}.txt
        Uses date from createdDateTime (not current time) for determinism.
        
        Args:
            item: Item from API with brandCode and createdDateTime
            
        Returns:
            Supplier filename, e.g., "BM_20260107.txt"
        """
        brand_code = item.get('brandCode', 'UNKNOWN')
        
        # Extract date from createdDateTime for deterministic naming
        created_date_str = item.get('createdDateTime', '')
        if created_date_str:
            try:
                dt = self._parse_date_string(created_date_str)
                if dt:
                    date_part = dt.strftime('%Y%m%d')
                    return f"{brand_code}_{date_part}.txt"
            except Exception as e:
                logger.warning(f"Could not parse createdDateTime '{created_date_str}': {e}")
        
        # Fallback: use brand code with placeholder
        logger.warning(f"No createdDateTime for {brand_code}, using fallback filename")
        return f"{brand_code}_current.txt"
    
    async def _download_item(
        self, client: httpx.Client, item: Dict[str, Any]
    ) -> Optional[ScrapedFile]:
        """
        Download a single price list file from the API.
        
        Calls GET /download-pricelist?brandCode=XX and saves the response.
        Retries once on 404 after 30 second wait (transient API issues).
        
        Args:
            client: HTTP client instance
            item: Item from API with brand info
            
        Returns:
            ScrapedFile object, or None if download failed
        """
        import asyncio
        
        api_cfg = self.config.get('api', {})
        export_endpoint = api_cfg.get('export_endpoint', '/download-pricelist')
        params_template = api_cfg.get('export_params_template', {'brandCode': '{brandCode}'})
        
        # Build request params from template
        params: Dict[str, Any] = {}
        for key, value in params_template.items():
            if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                field_name = value[1:-1]
                params[key] = item.get(field_name)
            else:
                params[key] = value
        
        logger.info(f"Downloading price list with params: {params}")
        
        # Retry logic for transient failures
        max_attempts = 2
        retry_delay = 30  # seconds
        response = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                response = client.get(export_endpoint, params=params)
                response.raise_for_status()
                
                # Success - break out of retry loop
                break
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt < max_attempts:
                    # Transient 404 - retry after delay
                    logger.warning(
                        f"HTTP 404 on attempt {attempt}/{max_attempts} for {params}. "
                        f"Retrying in {retry_delay}s (transient API issue)..."
                    )
                    await asyncio.sleep(retry_delay)
                    continue
                else:
                    # Final failure or non-404 error
                    if e.response.status_code == 404:
                        logger.error(
                            f"HTTP 404 after {max_attempts} attempts for {params}. "
                            f"Skipping file (persistent API issue)."
                        )
                    else:
                        logger.error(f"HTTP error downloading price list: {e}")
                    return None
                    
            except Exception as e:
                logger.error(f"Error downloading price list: {e}")
                return None
        
        # Verify we have a successful response
        if response is None:
            logger.error(f"No response received after {max_attempts} attempts for {params}")
            return None
        
        # Log response details
        content_type = response.headers.get('Content-Type', 'unknown')
        content_length = response.headers.get('Content-Length', 'unknown')
        logger.info(f"Response Content-Type: {content_type}, Content-Length: {content_length}")
        
        # Extract filename from Content-Disposition header or generate one
        filename = self._extract_filename_from_response(response, item)
        
        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)
        file_path = os.path.join(self.download_dir, filename)
        
        # Save file content
        logger.info(f"Saving file to: {file_path}")
        with open(file_path, 'wb') as f:
            for chunk in response.iter_bytes():
                f.write(chunk)
        
        # Build ScrapedFile with metadata using auto-detected fields
        if not self._detected_fields:
            self._detect_api_fields(item)
        
        # Fix lint errors #6-7: Handle case where self._detected_fields could be None
        brand_field = self._detected_fields.get('brand', 'brandCode') if self._detected_fields else 'brandCode'
        created_date_field = self._detected_fields.get('createdDate') if self._detected_fields else None
        
        raw_brand = item.get(brand_field) if brand_field else None
        # Use canonical brand name (resolved from alias) instead of raw brand code
        # This ensures filenames use full brand names (e.g., "HYUNDAI" instead of "HY")
        canonical_brand = item.get('_canonical_brand', raw_brand)
        
        kwargs: Dict[str, Any] = {'brand': canonical_brand}
        
        # Extract valid_from date
        if created_date_field:
            date_str = item.get(created_date_field)
            if date_str:
                kwargs['valid_from_date_str'] = date_str
                logger.info(f"Extracted valid_from date from '{created_date_field}': {date_str}")
        
        return self.create_scraped_file(
            filename=filename,
            local_path=file_path,
            **kwargs,
        )
    
    def _extract_filename_from_response(
        self, response: httpx.Response, item: Dict[str, Any]
    ) -> str:
        """
        Generate deterministic supplier filename for APF.
        
        MUST match the format used by _generate_expected_filename().
        
        Format: {brandCode}_{YYYYMMDD}.txt
        Uses date from createdDateTime (not current time) for determinism.
        
        Note: We deliberately SKIP Content-Disposition header because APF's API
        returns timestamp-based filenames that break duplicate detection.
        
        Args:
            response: HTTP response object
            item: Item data from API
            
        Returns:
            Supplier filename string
        """
        brand_code = item.get('brandCode', 'UNKNOWN')
        created_date_str = item.get('createdDateTime', '')
        
        # Generate deterministic filename using date from API
        if created_date_str:
            try:
                dt = self._parse_date_string(created_date_str)
                if dt:
                    date_part = dt.strftime('%Y%m%d')
                    filename = f"{brand_code}_{date_part}.txt"
                    logger.debug(f"Generated deterministic filename: {filename}")
                    return filename
                else:
                    logger.warning(f"Failed to parse createdDateTime '{created_date_str}'")
            except Exception as e:
                logger.error(f"Could not parse createdDateTime '{created_date_str}': {e}")
        else:
            logger.warning(f"No createdDateTime in item! Keys: {list(item.keys())}")
        
        # Fallback: use timestamp (should rarely happen)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"{brand_code}_{timestamp}.txt"
        logger.warning(f"Using timestamp fallback filename: {filename}")
        return filename
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """
        Parse date string to datetime object.
        
        Handles common formats including ISO8601.
        
        Args:
            date_str: Date string to parse
            
        Returns:
            datetime object with timezone, or None if parsing fails
        """
        if not date_str:
            return None
        
        try:
            from dateutil import parser as date_parser
            dt = date_parser.parse(date_str)
            
            # Ensure timezone aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            return dt
        except Exception as e:
            logger.debug(f"Failed to parse date string '{date_str}': {e}")
            return None

