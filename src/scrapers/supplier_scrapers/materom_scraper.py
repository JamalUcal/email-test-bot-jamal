"""
Materom-specific scraper for Nextcloud.

Uses WebDAV PROPFIND API to list files and direct WebDAV URLs to download.
This is more reliable than clicking buttons in the HTML interface.
"""

from typing import List, Dict, Any, Optional, AsyncIterator
import os
import asyncio
import xml.etree.ElementTree as ET
from urllib.parse import quote, unquote

from scrapers.scraper_base import BaseScraper, ScrapingResult, ScrapedFile
from scrapers.brand_matcher import find_matching_brand, load_brand_configs
from scrapers.version_detector import VersionDetector
from utils.logger import get_logger
from utils.credential_manager import CredentialManager, CredentialError

logger = get_logger(__name__)


class MateromScraper(BaseScraper):
    """Custom scraper for Materom Nextcloud instance."""
    
    def __init__(self, config: Dict[str, Any], browser_manager: Any, start_index: int = 0, state_manager: Optional[Any] = None):
        """Initialize Materom scraper with version detector for duplicate detection."""
        super().__init__(config, browser_manager, start_index, state_manager)
        self.version_detector = VersionDetector()
        self.total_files_found = 0  # Track total files before duplicate filtering
        self.files_skipped_duplicates = 0  # Track files skipped as duplicates
    
    async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
        """
        Stream scraped files one at a time for immediate processing.
        
        Yields files as they're downloaded for immediate CSV generation and Drive upload.
        
        Yields:
            ScrapedFile objects as they are downloaded
        """
        try:
            await self.browser_manager.start()
            
            # Step 1: Authenticate to get session cookies
            auth_ok = await self.authenticate()
            if not auth_ok:
                logger.error('Authentication failed')
                return
            
            # Step 2: Get cookies and CSRF token from browser for API requests
            page = self.browser_manager._ensure_page_initialized()
            cookies = await page.context.cookies()
            cookie_dict: Dict[str, str] = {}
            for cookie in cookies:
                name = cookie.get('name')
                value = cookie.get('value')
                if name and value:
                    cookie_dict[name] = value
            logger.info(f"Got {len(cookie_dict)} cookies from browser session")
            logger.info(f"Cookie names: {list(cookie_dict.keys())}")
            
            # Extract CSRF token (requesttoken) from page
            request_token: Optional[str] = None
            try:
                # Nextcloud stores requesttoken in a meta tag or data attribute
                request_token = await page.evaluate("""
                    () => {
                        // Try meta tag first
                        const meta = document.querySelector('meta[name="csrf-token"]');
                        if (meta) return meta.getAttribute('content');
                        
                        // Try data attribute on body
                        const body = document.body;
                        if (body && body.dataset.requesttoken) return body.dataset.requesttoken;
                        
                        // Try OC.requestToken global variable
                        if (typeof OC !== 'undefined' && OC.requestToken) return OC.requestToken;
                        
                        return null;
                    }
                """)
                if request_token:
                    logger.info(f"Extracted CSRF token: {request_token[:8]}...")
                else:
                    logger.warning("Could not extract CSRF token from page")
            except Exception as e:
                logger.warning(f"Failed to extract CSRF token: {e}")
            
            # Step 3: List files via WebDAV API
            files_list = await self._list_files_webdav(cookie_dict, request_token)
            logger.info(f"WebDAV file listing complete: {len(files_list)} total files")
            if files_list:
                logger.info(f"Sample files: {[f['filename'] for f in files_list[:3]]}")
            if not files_list:
                logger.warning("No files found via WebDAV")
                return
            
            # Step 4: Filter files by brand
            files_to_download = await self._filter_files_by_brand(files_list)
            
            # Track total files found (after brand filtering, before duplicate checking)
            self.total_files_found = len(files_to_download)
            
            logger.info(f"Brand filtering complete: {len(files_to_download)}/{len(files_list)} files match config brands")
            if not files_to_download and files_list:
                logger.warning(f"All {len(files_list)} files were filtered out. Check brand matching logic.")
                # Extract config brands for logging
                config_brands = self.config.get('config', [])
                brands_list = [cfg['brand'] for cfg in config_brands if cfg.get('enabled', True)]
                logger.warning(f"Config brands: {brands_list}")
            
            if not files_to_download:
                logger.warning("No files matched after filtering")
                return
            
            # Step 5: Download and yield files one at a time
            for idx, file_info in enumerate(files_to_download, 1):
                # Skip if resuming from interruption
                if idx <= self.start_index:
                    logger.info(f"[RESUME] Skipping file {idx}/{len(files_to_download)} (resuming from index {self.start_index})")
                    continue
                
                supplier_filename = file_info['filename']
                
                # Extract date/version from filename and metadata for duplicate detection
                valid_from_date_str: Optional[str] = None
                
                if self.state_manager:
                    # Try to detect version/date from filename and metadata
                    detection_mode = self.config.get('version_detection', {}).get('mode', 'date_based')
                    
                    # Build item dict with both filename and metadata
                    item_dict = {
                        'filename': supplier_filename,
                    }
                    # Include modified date if available (from WebDAV PROPFIND)
                    if 'modified' in file_info:
                        item_dict['modified'] = file_info['modified']
                    
                    version = self.version_detector.detect_version(
                        item=item_dict,
                        detection_mode=detection_mode
                    )
                    
                    if version:
                        valid_from_date_str = version
                    
                    logger.info(
                        f"[DUPLICATE CHECK] File {idx}/{len(files_to_download)}: supplier_filename={supplier_filename}, valid_from={valid_from_date_str}",
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
                            self.files_skipped_duplicates += 1  # Track duplicate skip
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(files_to_download)} - Already have: {supplier_filename} valid from {valid_from_date_str}. NOT downloading.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(files_to_download)} - Not in state: {supplier_filename} valid from {valid_from_date_str}. Will download.",
                                supplier_filename=supplier_filename,
                                valid_from_date=valid_from_date_str
                            )
                    elif supplier_filename:
                        # No date detected - check by filename only
                        if self.state_manager.is_file_already_processed(
                            supplier=self.supplier_name,
                            supplier_filename=supplier_filename
                        ):
                            self.files_skipped_duplicates += 1  # Track duplicate skip
                            logger.info(
                                f"[DUPLICATE SKIP] File {idx}/{len(files_to_download)} - Already have: {supplier_filename} (no date). NOT downloading.",
                                supplier_filename=supplier_filename
                            )
                            continue
                        else:
                            logger.info(
                                f"[NEW FILE] File {idx}/{len(files_to_download)} - Not in state: {supplier_filename} (no date). Will download.",
                                supplier_filename=supplier_filename
                            )
                
                logger.info(f"[DOWNLOAD] Downloading file {idx}/{len(files_to_download)}: {supplier_filename}")
                
                file_path = await self._download_file_webdav(file_info, cookie_dict, request_token)
                if file_path:
                    brand = file_info.get('brand', '')
                    # Pass original filename and valid_from_date_str for duplicate detection
                    scraped_file = self.create_scraped_file(
                        supplier_filename, 
                        file_path, 
                        brand=brand,
                        supplier_filename=supplier_filename,  # Original filename from Nextcloud
                        valid_from_date_str=valid_from_date_str  # Date/version for duplicate detection
                    )
                    logger.info(f"Successfully downloaded: {supplier_filename}")
                    yield scraped_file
                else:
                    logger.error(f"Failed to download: {supplier_filename}")
                
                # Small delay between downloads
                if idx < len(files_to_download):
                    await asyncio.sleep(1)
        
        except Exception as e:
            logger.error(f"Materom streaming scraping failed: {str(e)}")
            raise
        
        finally:
            await self.browser_manager.close()
    
    async def scrape(self) -> ScrapingResult:
        """
        Scrape Materom Nextcloud by:
        1. Logging in via browser to get session cookies
        2. Using WebDAV PROPFIND API to list files
        3. Filtering by brand
        4. Downloading via WebDAV URLs
        """
        import time
        start_time = time.time()
        result = ScrapingResult(supplier=self.supplier_name, success=False)
        
        try:
            await self.browser_manager.start()
            
            # Step 1: Authenticate to get session cookies
            auth_ok = await self.authenticate()
            if not auth_ok:
                result.errors.append('Authentication failed')
                return result
            
            # Step 2: Get cookies and CSRF token from browser for API requests
            page = self.browser_manager._ensure_page_initialized()
            cookies = await page.context.cookies()
            cookie_dict: Dict[str, str] = {}
            for cookie in cookies:
                name = cookie.get('name')
                value = cookie.get('value')
                if name and value:
                    cookie_dict[name] = value
            logger.info(f"Got {len(cookie_dict)} cookies from browser session")
            logger.info(f"Cookie names: {list(cookie_dict.keys())}")
            
            # Extract CSRF token (requesttoken) from page
            request_token: Optional[str] = None
            try:
                # Nextcloud stores requesttoken in a meta tag or data attribute
                request_token = await page.evaluate("""
                    () => {
                        // Try meta tag first
                        const meta = document.querySelector('meta[name="csrf-token"]');
                        if (meta) return meta.getAttribute('content');
                        
                        // Try data attribute on body
                        const body = document.body;
                        if (body && body.dataset.requesttoken) return body.dataset.requesttoken;
                        
                        // Try OC.requestToken global variable
                        if (typeof OC !== 'undefined' && OC.requestToken) return OC.requestToken;
                        
                        return null;
                    }
                """)
                if request_token:
                    logger.info(f"Extracted CSRF token: {request_token[:8]}...")
                else:
                    logger.warning("Could not extract CSRF token from page")
            except Exception as e:
                logger.warning(f"Failed to extract CSRF token: {e}")
            
            # Step 3: List files via WebDAV API
            files_list = await self._list_files_webdav(cookie_dict, request_token)
            if not files_list:
                logger.warning("No files found via WebDAV")
                result.success = True
                return result
            
            logger.info(f"Found {len(files_list)} files via WebDAV")
            
            # Step 4: Filter files by brand
            files_to_download = await self._filter_files_by_brand(files_list)
            
            if not files_to_download:
                logger.warning("No files matched after filtering")
                result.success = True
                return result
            
            logger.info(f"Found {len(files_to_download)} files to download after filtering")
            
            # Step 5: Download each file
            downloaded_files: List[ScrapedFile] = []
            
            for idx, file_info in enumerate(files_to_download, 1):
                supplier_filename = file_info['filename']
                logger.info(f"Downloading file {idx}/{len(files_to_download)}: {supplier_filename}")
                
                # Extract date/version from filename and metadata for duplicate detection
                valid_from_date_str: Optional[str] = None
                detection_mode = self.config.get('version_detection', {}).get('mode', 'date_based')
                
                # file_info already has both filename and modified (from _filter_files_by_brand)
                version = self.version_detector.detect_version(
                    item=file_info,
                    detection_mode=detection_mode
                )
                
                if version:
                    valid_from_date_str = version
                
                file_path = await self._download_file_webdav(file_info, cookie_dict, request_token)
                if file_path:
                    brand = file_info.get('brand', '')
                    # Pass original filename and valid_from_date_str for duplicate detection
                    scraped_file = self.create_scraped_file(
                        supplier_filename, 
                        file_path, 
                        brand=brand,
                        supplier_filename=supplier_filename,  # Original filename from Nextcloud
                        valid_from_date_str=valid_from_date_str  # Date/version for duplicate detection
                    )
                    downloaded_files.append(scraped_file)
                    logger.info(f"Successfully downloaded: {supplier_filename}")
                else:
                    logger.error(f"Failed to download: {supplier_filename}")
                
                # Small delay between downloads
                if idx < len(files_to_download):
                    await asyncio.sleep(1)
            
            result.files = downloaded_files
            result.success = len(downloaded_files) > 0
            
        except Exception as e:
            logger.error(f"Materom scraping failed: {str(e)}")
            result.errors.append(str(e))
        finally:
            await self.browser_manager.close()
            result.execution_time_seconds = time.time() - start_time
        
        return result
    
    async def authenticate(self) -> bool:
        """Authenticate using form-based login."""
        auth_cfg = self.config.get('authentication', {})
        if auth_cfg.get('method') != 'form':
            return True
        
        login_url = auth_cfg.get('login_url')
        if login_url:
            ok = await self.browser_manager.navigate(login_url)
            if not ok:
                return False
        
        await self.browser_manager.take_screenshot("00_before_login")
        
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
        
        # Fill and submit form
        result = await self.browser_manager.fill_form(
            username_field=auth_cfg.get('username_field'),
            password_field=auth_cfg.get('password_field'),
            username=username,
            password=password,
            submit_button=auth_cfg.get('submit_button')
        )
        
        await self.browser_manager.take_screenshot("00_after_login")
        
        return result
    
    async def navigate_to_downloads(self) -> bool:
        """Not used with WebDAV approach."""
        return True
    
    async def download_files(self) -> List[ScrapedFile]:
        """Not used - downloading is handled in scrape()."""
        return []
    
    async def _list_files_webdav(self, cookie_dict: Dict[str, str], request_token: Optional[str] = None) -> List[Dict[str, str]]:
        """
        List files in Share Folder via WebDAV PROPFIND API using HTTP Basic Auth.
        
        Args:
            cookie_dict: Session cookies (not used for WebDAV, kept for signature compatibility)
            request_token: CSRF token (not used for WebDAV, kept for signature compatibility)
        
        Returns:
            List of dicts with 'filename' and 'href'
        """
        import aiohttp
        
        # Retrieve and validate credentials using CredentialManager
        auth_cfg = self.config.get('authentication', {})
        try:
            cred_manager = CredentialManager(self.supplier_name, auth_cfg)
            credentials = cred_manager.get_credentials()
            username = credentials.username
            password = credentials.password
        except CredentialError as e:
            logger.error(f"Credential validation failed for WebDAV: {e}")
            return []
        
        # Construct WebDAV URL
        links_cfg = self.config.get('links', {})
        page_url = links_cfg.get('page_url', '')
        from urllib.parse import urlparse
        parsed = urlparse(page_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        webdav_url = f"{base_url}/remote.php/dav/files/{username}/Share%20Folder/"
        
        # PROPFIND request body
        propfind_xml = '''<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:nc="http://nextcloud.org/ns" xmlns:oc="http://owncloud.org/ns">
    <d:prop>
        <d:displayname />
        <d:getcontenttype />
        <d:resourcetype />
        <d:getlastmodified />
    </d:prop>
</d:propfind>'''
        
        try:
            logger.info(f"WebDAV PROPFIND request to: {webdav_url}")
            
            # Get timeout from config
            list_timeout = self.config.get('execution', {}).get('webdav_list_timeout_seconds', 60)
            
            logger.info(f"Using HTTP Basic Auth for WebDAV with username: {username[:3]}***")
            auth = aiohttp.BasicAuth(username, password)
            
            headers = {
                'Content-Type': 'application/xml'
            }
            
            # Use session with Basic Auth
            async with aiohttp.ClientSession(auth=auth) as session:
                async with session.request(
                    'PROPFIND',
                    webdav_url,
                    data=propfind_xml,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=list_timeout)
                ) as response:
                    logger.info(f"WebDAV response status: {response.status}")
                    if response.status != 207:  # Multi-Status
                        logger.error(f"WebDAV PROPFIND failed: {response.status}")
                        response_text = await response.text()
                        logger.error(f"Response body: {response_text[:500]}")
                        return []
                    
                    xml_content = await response.text()
                    logger.info(f"WebDAV response length: {len(xml_content)} bytes")
                    
                    # Parse XML response
                    files = self._parse_propfind_response(xml_content)
                    logger.info(f"Parsed {len(files)} files from WebDAV response")
                    return files
        
        except Exception as e:
            logger.error(f"WebDAV request failed: {str(e)}")
            return []
    
    def _parse_propfind_response(self, xml_content: str) -> List[Dict[str, str]]:
        """Parse WebDAV PROPFIND XML response to extract files with metadata."""
        files: List[Dict[str, str]] = []
        
        try:
            root = ET.fromstring(xml_content)
            
            # Define namespaces
            ns = {
                'd': 'DAV:',
                'oc': 'http://owncloud.org/ns',
                'nc': 'http://nextcloud.org/ns'
            }
            
            # Find all response elements
            for response in root.findall('.//d:response', ns):
                href_elem = response.find('d:href', ns)
                if href_elem is None:
                    continue
                
                href = href_elem.text
                if not href:
                    continue
                
                # Check if it's a file (not a directory)
                resourcetype = response.find('.//d:resourcetype', ns)
                if resourcetype is not None and len(resourcetype) > 0:
                    # Has children = it's a collection/directory
                    continue
                
                # Extract filename from href
                filename = unquote(href.split('/')[-1])
                
                # Only include .xlsx files
                if filename.endswith('.xlsx'):
                    file_info: Dict[str, str] = {
                        'filename': filename,
                        'href': href
                    }
                    
                    # Extract last modified date if available
                    lastmodified_elem = response.find('.//d:getlastmodified', ns)
                    if lastmodified_elem is not None and lastmodified_elem.text:
                        file_info['modified'] = lastmodified_elem.text
                    
                    files.append(file_info)
            
            logger.info(f"Parsed {len(files)} xlsx files from WebDAV response")
            return files
        
        except Exception as e:
            logger.error(f"Failed to parse WebDAV XML: {str(e)}")
            return []
    
    async def _filter_files_by_brand(self, files_list: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Filter file list by brand, respecting config array filter.
        
        Filtering steps:
        1. Skip special files
        2. Extract brand from filename
        3. Filter by config array if configured
        4. Match against brand_config.json
        
        Args:
            files_list: List of dicts with 'filename' and 'href' from WebDAV
        
        Returns:
            List of dicts with 'filename', 'brand', and 'href'
        """
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
        
        # Log brand extraction pattern being used
        brand_pattern = self.config.get('brand_pattern', '^([A-Z]+)_')
        logger.info(f"Using brand extraction pattern: {brand_pattern}")
        
        brand_configs = load_brand_configs()
        if not brand_configs:
            logger.error("Failed to load brand configs")
            return []
        
        filtered_files: List[Dict[str, Any]] = []
        skipped_files: Dict[str, int] = {}
        matched_brands: Dict[str, int] = {}
        config_filtered_brands: Dict[str, int] = {}
        
        for file_info in files_list:
            filename = file_info['filename']
            
            # Skip special files
            if filename in ['Available stock.xlsx', '_Stock Clearance - we accept target prices.xlsx']:
                logger.info(f"Skipping special file: {filename}")
                continue
            
            # Extract brand from filename
            brand = self._extract_brand_from_filename(filename)
            if not brand:
                logger.warning(f"Could not extract brand from filename: {filename}")
                continue
            
            # First resolve brand via aliases to get canonical brand name
            matched_config = find_matching_brand(brand, brand_configs)
            
            logger.debug(f"File: {filename} -> extracted_brand: {brand} -> matched: {matched_config is not None}")
            
            if not matched_config:
                # Brand not found in brand_config.json
                skipped_files[brand] = skipped_files.get(brand, 0) + 1
                continue
            
            canonical_brand = matched_config.get('brand', brand)
            
            # Second filter: Check config array filter if configured
            if active_brands:
                if canonical_brand.upper() not in active_brands_upper:
                    # Canonical brand not in config array, skip
                    config_filtered_brands[canonical_brand] = config_filtered_brands.get(canonical_brand, 0) + 1
                    continue
            
            # Brand passed all filters - preserve all metadata
            filtered_file_info = {
                'filename': filename,
                'brand': canonical_brand,
                'href': file_info['href']
            }
            
            # Preserve modified date if available (critical for version detection)
            if 'modified' in file_info:
                filtered_file_info['modified'] = file_info['modified']
            
            filtered_files.append(filtered_file_info)
            
            matched_brands[canonical_brand] = matched_brands.get(canonical_brand, 0) + 1
        
        # Log filtering results
        logger.info(f"Brand filtering: {len(filtered_files)}/{len(files_list)} files matched")
        
        if config_filtered_brands:
            filtered_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(config_filtered_brands.items())])
            logger.info(f"Filtered by config array: {filtered_summary}")
        
        if matched_brands:
            matched_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(matched_brands.items())])
            logger.info(f"Matched brands: {matched_summary}")
        
        if skipped_files:
            skipped_summary = ", ".join([f"{brand} ({count})" for brand, count in sorted(skipped_files.items())])
            logger.warning(f"Skipped brands (not in brand_config.json): {skipped_summary}")
        
        return filtered_files
    
    def _extract_brand_from_filename(self, filename: str) -> Optional[str]:
        """
        Extract brand from filename using multiple patterns.
        
        The extracted brand will be matched against brand_config.json aliases
        to find the canonical brand name.
        
        Examples:
        - "ATE_2025.xlsx" -> "ATE"
        - "Corteco_2025.xlsx" -> "CORTECO"
        - "BorgWarner - Beru_2025.xlsx" -> "BORGWARNER"
        - "CASTROL ARAL PRICE LIST_ OCT_2025.xlsx" -> "CASTROL"
        - "MERCEDESBENZ_2025.xlsx" -> "MERCEDESBENZ" (will be matched via aliases)
        - "VWVAG_2025.xlsx" -> "VWVAG" (will be matched via aliases)
        """
        import re
        
        # Try multiple patterns in order of specificity
        patterns = [
            r'^([A-Z][A-Za-z]+)_',  # Corteco_2025.xlsx -> CORTECO
            r'^([A-Z][A-Za-z\s]+)\s*-',  # BorgWarner - Beru -> BORGWARNER
            r'^([A-Z][A-Z\s]+)\s',  # CASTROL ARAL -> CASTROL
            r'^([A-Z]+)_',  # ATE_2025 -> ATE
            r'^([A-Z][A-Za-z]+)\.',  # Corteco.xlsx -> CORTECO (no underscore)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                brand = match.group(1).strip()
                # Remove trailing spaces and convert to uppercase
                brand = brand.replace(' ', '').upper()
                return brand
        
        return None
    
    async def _download_file_webdav(self, file_info: Dict[str, Any], cookie_dict: Dict[str, str], request_token: Optional[str] = None) -> Optional[str]:
        """
        Download a file via WebDAV URL using HTTP Basic Auth.
        
        Args:
            file_info: Dict with 'filename', 'brand', and 'href'
            cookie_dict: Session cookies (not used for WebDAV, kept for signature compatibility)
            request_token: CSRF token (not used for WebDAV, kept for signature compatibility)
            
        Returns:
            Local file path or None if download failed
        """
        import aiohttp
        
        filename = file_info['filename']
        href = file_info['href']
        
        try:
            # Construct full WebDAV URL
            links_cfg = self.config.get('links', {})
            page_url = links_cfg.get('page_url', '')
            from urllib.parse import urlparse
            parsed = urlparse(page_url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            download_url = f"{base_url}{href}"
            
            logger.info(f"Downloading via WebDAV: {download_url}")
            
            # Get timeout from config
            download_timeout = self.config.get('execution', {}).get('download_timeout_seconds', 300)
            
            # Retrieve and validate credentials using CredentialManager
            auth_cfg = self.config.get('authentication', {})
            try:
                cred_manager = CredentialManager(self.supplier_name, auth_cfg)
                credentials = cred_manager.get_credentials()
                username = credentials.username
                password = credentials.password
            except CredentialError as e:
                logger.error(f"Credential validation failed for WebDAV download: {e}")
                return None
            
            auth = aiohttp.BasicAuth(username, password)
            
            # Use session with Basic Auth
            async with aiohttp.ClientSession(auth=auth) as session:
                async with session.get(
                    download_url,
                    timeout=aiohttp.ClientTimeout(total=download_timeout)
                ) as response:
                    if response.status != 200:
                        logger.error(f"Download failed with status: {response.status}")
                        return None
                    
                    # Save to file
                    file_path = os.path.join(self.browser_manager.download_dir, filename)
                    
                    with open(file_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
                    
                    logger.info(f"Download complete: {filename} ({os.path.getsize(file_path)} bytes)")
                    return file_path
        
        except asyncio.TimeoutError:
            timeout = self.config.get('execution', {}).get('download_timeout_seconds', 300)
            logger.error(f"Download timeout for {filename} (exceeded {timeout}s)")
            return None
        except Exception as e:
            logger.error(f"Failed to download {filename}: {str(e)}")
            return None

