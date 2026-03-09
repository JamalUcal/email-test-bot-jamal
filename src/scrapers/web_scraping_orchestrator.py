"""
Web scraping orchestrator - coordinates website scraping workflow.

Manages the complete scraping pipeline:
1. Check which suppliers are scheduled to run
2. Execute scrapers for each supplier
3. Process downloaded files through existing parsing pipeline
4. Update state and generate reports
"""

import asyncio
import os
from typing import Dict, List, Optional, Any, cast
from dataclasses import dataclass, field
from datetime import datetime, timezone
import tempfile
from pathlib import Path

from scrapers.browser_manager import BrowserManager
from scrapers.scraper_factory import ScraperFactory
from scrapers.scraper_base import ScrapingResult, ScrapedFile
from scrapers.execution_monitor import ExecutionMonitor
from scrapers.schedule_evaluator import ScheduleEvaluator
from scrapers.version_detector import VersionDetector
from parsers.brand_detector import BrandDetector
from parsers.currency_detector import CurrencyDetector
from parsers.date_parser import DateParser
from parsers.price_list_parser import PriceListParser
from output.file_generator import FileGenerator
from output.drive_uploader import DriveUploader
from storage.bigquery_processor import BigQueryPriceListProcessor
from utils.logger import get_logger
from utils.state_manager import StateManager
from utils.exceptions import EmailProcessingError
from utils.config_merger import ConfigMerger

logger = get_logger(__name__)


@dataclass
class ScrapedFileOutput:
    """Result of processing a scraped file."""
    filename: str
    local_path: str
    drive_file_id: Optional[str] = None
    drive_link: Optional[str] = None
    brand: Optional[str] = None
    supplier: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    total_rows: int = 0
    valid_rows: int = 0
    parsing_errors_count: int = 0
    # BigQuery reconciliation info (when enabled)
    price_list_id: Optional[str] = None
    reconciliation_stats: Optional[Dict[str, Any]] = None


@dataclass
class SupplierScrapingResult:
    """Complete result of scraping one supplier."""
    supplier: str
    scraping_result: ScrapingResult
    files_processed: List[ScrapedFileOutput] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    scraper_url: Optional[str] = None  # URL used for scraping (if applicable)
    files_skipped_duplicates: int = 0  # Number of files skipped due to duplicate detection
    total_files_found: int = 0  # Total files found before duplicate filtering


class WebScrapingOrchestrator:
    """Orchestrates the complete website scraping workflow."""
    
    def __init__(
        self,
        scraper_configs: List[Dict],
        supplier_configs: List[Dict],
        brand_configs: List[Dict],
        core_config: Dict,
        service_account_info: Dict,
        column_mapping_config: Dict,
        currency_config: Dict,
        state_manager: Optional[StateManager] = None
    ):
        """
        Initialize scraping orchestrator.
        
        Args:
            scraper_configs: List of scraper configurations
            supplier_configs: List of supplier configurations
            brand_configs: List of brand configurations
            core_config: Core configuration
            service_account_info: Dict with service account credentials
            column_mapping_config: Column mapping configuration for header detection (REQUIRED)
            currency_config: Currency configuration for currency detection (REQUIRED)
            state_manager: Optional state manager for tracking scraper progress
        """
        self.scraper_configs = scraper_configs
        self.supplier_configs = supplier_configs
        self.brand_configs = brand_configs
        self.core_config = core_config
        self.state_manager = state_manager
        
        # Initialize components
        self.brand_detector = BrandDetector(brand_configs)
        self.currency_detector = CurrencyDetector(currency_config)
        self.date_parser = DateParser(timezone=core_config['execution'].get('timezone', 'UTC'))
        self.parser = PriceListParser(
            column_mapping_config=column_mapping_config,
            currency_config=currency_config
        )
        self.file_generator = FileGenerator(column_mapping_config=column_mapping_config)
        self.schedule_evaluator = ScheduleEvaluator()
        self.version_detector = VersionDetector()
        
        # Initialize Drive uploader with delegated credentials
        drive_impersonation_email = core_config.get('drive', {}).get('impersonation_email')
        self.drive_uploader = DriveUploader(
            service_account_info=service_account_info,
            delegated_user=drive_impersonation_email
        )
        
        # Initialize BigQuery processor (required for supersession reconciliation)
        bigquery_config = core_config['bigquery']
        self.bq_processor = BigQueryPriceListProcessor(
            project_id=bigquery_config['project_id'],
            dataset_id=bigquery_config['dataset_id'],
            staging_bucket=bigquery_config.get('staging_bucket', core_config['gcp']['bucket_name']),
            service_account_info=service_account_info,
            bigquery_config=bigquery_config
        )
        
        # Create brand config lookup
        self.brand_config_map = {
            config['brand'].upper(): config
            for config in brand_configs
        }
        
        # Log sample of brand configs with drive folder IDs for verification
        brands_with_folders = [
            (config['brand'], config.get('driveFolderId', 'MISSING'))
            for config in brand_configs
            if config.get('driveFolderId')
        ]
        logger.info(
            "WebScrapingOrchestrator initialized",
            version="2025-11-08_v2_diagnostic",
            scrapers=len(scraper_configs),
            suppliers=len(supplier_configs),
            brands=len(brand_configs),
            state_tracking=state_manager is not None,
            sample_brands_with_folders=brands_with_folders[:5]  # Log first 5 for verification
        )
        
        # Create supplier config lookup
        self.supplier_config_map = {
            config['supplier']: config
            for config in supplier_configs
        }
    
    async def process_scheduled_scrapers(
        self,
        dry_run: bool = False,
        force: bool = False
    ) -> List[SupplierScrapingResult]:
        """
        Process all suppliers that are scheduled to run.
        
        Args:
            dry_run: If True, skip Drive upload
            force: If True, skip schedule checks and process all enabled scrapers
            
        Returns:
            List of SupplierScrapingResult objects
        """
        results = []
        
        for scraper_config in self.scraper_configs:
            if not scraper_config.get('enabled', False):
                continue
            
            supplier_name = scraper_config['supplier']
            
            # Check if supplier is scheduled to run (skip if force=True)
            if not force and not self._is_supplier_scheduled(scraper_config):
                logger.debug(f"Supplier {supplier_name} not scheduled to run")
                continue
            
            logger.info(f"Processing scheduled scraper for {supplier_name}")
            
            # Check for interrupted state and resume if needed
            start_index = 0
            if self.state_manager:
                supplier_state = self.state_manager.get_supplier_state(supplier_name)
                if supplier_state.get('interrupted', False):
                    start_index = supplier_state.get('last_file_index', 0)
                    logger.info(
                        f"Resuming interrupted scraper for {supplier_name}",
                        start_index=start_index
                    )
            
            try:
                result = await self._process_supplier_scraper(
                    scraper_config=scraper_config,
                    dry_run=dry_run,
                    start_index=start_index
                )
                results.append(result)
                
                # Clear interrupted flag after successful completion
                if self.state_manager and not result.errors:
                    self.state_manager.clear_supplier_interrupted(supplier_name)
                
            except Exception as e:
                import traceback
                error_msg = f"Scraper execution failed: {str(e)}"
                logger.error(f"Failed to process scraper for {supplier_name}: {str(e)}")
                logger.error(f"Exception type: {type(e).__name__}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
                
                # Create error result with errors in BOTH places
                try:
                    error_result = SupplierScrapingResult(
                        supplier=supplier_name,
                        scraping_result=ScrapingResult(
                            supplier=supplier_name,
                            success=False,
                            errors=[error_msg]
                        ),
                        errors=[error_msg]  # CRITICAL: Also add to SupplierScrapingResult.errors
                    )
                    results.append(error_result)
                except Exception as creation_error:
                    logger.error(f"CRITICAL: Failed to create error result object: {creation_error}")
                    logger.error(f"Creation error traceback: {traceback.format_exc()}")
        
        logger.info(f"Completed processing {len(results)} suppliers")
        return results
    
    async def _process_supplier_scraper(
        self,
        scraper_config: Dict,
        dry_run: bool,
        start_index: int = 0,
        skip_bigquery: bool = False
    ) -> SupplierScrapingResult:
        """
        Process a single supplier scraper.
        
        Args:
            scraper_config: Scraper configuration
            dry_run: Skip Drive upload if True
            start_index: Index to resume from (for interrupted runs)
            skip_bigquery: Skip BigQuery upload and supersession reconciliation if True
            
        Returns:
            SupplierScrapingResult
        """
        supplier_name = scraper_config['supplier']
        scraper_type = scraper_config.get('type', 'unknown')
        
        # Validate that supplier has brand-specific config array
        # Exception: Custom scrapers may not need a config array (e.g., email_trigger types)
        requires_config = scraper_type not in ['custom', 'email_trigger']
        
        if requires_config and ('config' not in scraper_config or not scraper_config['config']):
            error_msg = (
                f"CONFIGURATION ERROR: Supplier '{supplier_name}' (type: {scraper_type}) is missing required 'config' array "
                f"in scraper_config.json. This array must contain brand-specific configurations "
                f"(location, currency) for each enabled brand. Cannot proceed without it."
            )
            logger.error(error_msg)
            return SupplierScrapingResult(
                supplier=supplier_name,
                scraping_result=ScrapingResult(
                    supplier=supplier_name,
                    success=False,
                    errors=[error_msg]
                ),
                files_processed=[],
                warnings=[],
                errors=[error_msg]
            )
        
        # Extract HTTP Basic Auth credentials if needed
        http_credentials = None
        auth_config = scraper_config.get('authentication', {})
        auth_method = auth_config.get('method')
        
        if auth_method == 'basic':
            username_env = auth_config.get('username_env')
            password_env = auth_config.get('password_env')
            
            if username_env and password_env:
                import os
                username = os.getenv(username_env)
                password = os.getenv(password_env)
                
                if username and password:
                    http_credentials = {
                        'username': username,
                        'password': password
                    }
                    logger.info(f"HTTP Basic Auth credentials configured for {supplier_name}")
                else:
                    logger.warning(
                        f"HTTP Basic Auth credentials not found for {supplier_name}: "
                        f"{username_env}={username is not None}, {password_env}={password is not None}"
                    )
        
        # Create browser manager
        browser_manager = BrowserManager(
            headless=True,  # Always headless in production
            download_dir=f"./.scraper_downloads/{supplier_name}",
            screenshot_dir=f"./.scraper_screenshots/{supplier_name}",
            http_credentials=http_credentials
        )
        
        try:
            # Create scraper factory and scraper
            factory = ScraperFactory(browser_manager)
            scraper = factory.create_scraper(
                scraper_config=scraper_config,
                start_index=start_index,
                state_manager=self.state_manager
            )
            
            if not scraper:
                raise Exception(f"Failed to create scraper for {supplier_name}")
            
            # Run scraper and process files immediately as they're downloaded
            files_processed = []
            scraping_result = ScrapingResult(supplier=supplier_name, success=False)
            total_files_yielded = 0  # Track total files before filtering for duplicate detection
            
            async with browser_manager:
                # Check if scraper supports streaming (has scrape_stream method)
                # Not all scrapers implement streaming, so we check dynamically at runtime
                if hasattr(scraper, 'scrape_stream'):
                    logger.info(f"Using streaming mode for {supplier_name}")
                    file_count = start_index  # Start counting from where we left off
                    scraper_any = cast(Any, scraper)  # Cast to Any for dynamic method access
                    try:
                        async for scraped_file in scraper_any.scrape_stream():
                            total_files_yielded += 1  # Count every file yielded by scraper
                            file_count += 1
                            
                            try:
                                file_output = await self._process_scraped_file(
                                    scraped_file=scraped_file,
                                    scraper_config=scraper_config,
                                    dry_run=dry_run,
                                    skip_bigquery=skip_bigquery
                                )
                                if file_output:
                                    files_processed.append(file_output)
                                    logger.info(
                                        f"✓ Completed {len(files_processed)} file(s) for {supplier_name}: "
                                        f"{file_output.filename}"
                                    )
                                    
                                    # Update progress and record downloaded file
                                    if self.state_manager and not dry_run and not file_output.error:
                                        self.state_manager.update_file_progress(
                                            supplier=supplier_name,
                                            file_index=file_count,
                                            total_files=file_count
                                        )
                                        
                                        # Record file with brand and date for future duplicate detection
                                        # Use raw date strings from supplier (no conversion)
                                        valid_from_str = scraped_file.valid_from_date_str
                                        valid_to_str = scraped_file.expiry_date_str
                                        
                                        # If no valid_from date from API, try to detect from filename
                                        # Use original supplier filename if available (has the original date format)
                                        if not valid_from_str:
                                            detection_filename = scraped_file.supplier_filename or scraped_file.filename
                                            if detection_filename:
                                                detection_mode = scraper_config.get('schedule', {}).get('detection_mode', 'date_based')
                                                if detection_mode == 'date_based':
                                                    version = self.version_detector.detect_version(
                                                        item={'filename': detection_filename},
                                                        detection_mode=detection_mode
                                                    )
                                                    if version:
                                                        valid_from_str = version
                                                        logger.info(f"Detected date from filename for state tracking: {version}", filename=detection_filename)
                                        
                                        self.state_manager.add_downloaded_file(
                                            supplier=supplier_name,
                                            supplier_filename=scraped_file.supplier_filename or scraped_file.filename,
                                            valid_from_date=valid_from_str,
                                            drive_file_id=file_output.drive_file_id
                                        )
                            except Exception as e:
                                error_msg = f"Failed to process file {scraped_file.filename}: {str(e)}"
                                logger.error(error_msg)
                                scraping_result.errors.append(error_msg)
                    except Exception as stream_error:
                        import traceback
                        error_msg = f"Scraper stream failed: {str(stream_error)}"
                        logger.error(error_msg)
                        logger.error(f"Stream exception type: {type(stream_error).__name__}")
                        logger.error(f"Stream traceback: {traceback.format_exc()}")
                        scraping_result.errors.append(error_msg)
                        # Mark as failed - exception during streaming is a failure
                        scraping_result.success = False
                    else:
                        # No exception - mark as successful if we got any files OR if we found files but all were duplicates
                        # Check if scraper tracks its own file counts (for scrapers that do duplicate checking internally)
                        if hasattr(scraper_any, 'total_files_found') and scraper_any.total_files_found > 0:
                            scraping_result.total_files_found = scraper_any.total_files_found
                            scraping_result.files_skipped_duplicates = scraper_any.files_skipped_duplicates
                            logger.info(f"Scraper reported: {scraping_result.total_files_found} files found, {scraping_result.files_skipped_duplicates} skipped as duplicates")
                        else:
                            # Fallback: use orchestrator's count
                            scraping_result.total_files_found = total_files_yielded
                            scraping_result.files_skipped_duplicates = total_files_yielded - len(files_processed)
                        
                        scraping_result.success = scraping_result.total_files_found > 0 or len(files_processed) > 0
                else:
                    # Fallback to batch mode for scrapers that don't support streaming
                    logger.info(f"Using batch mode for {supplier_name}")
                    scraping_result = await scraper.scrape()
                    
                    if scraping_result.success and scraping_result.files:
                        total_files = len(scraping_result.files) + start_index  # Account for skipped files
                        
                        # Note: Scraper already skipped files, so files list contains only unprocessed items
                        for idx, scraped_file in enumerate(scraping_result.files, start_index + 1):
                            try:
                                file_output = await self._process_scraped_file(
                                    scraped_file=scraped_file,
                                    scraper_config=scraper_config,
                                    dry_run=dry_run,
                                    skip_bigquery=skip_bigquery
                                )
                                if file_output:
                                    files_processed.append(file_output)
                                    
                                    # Update progress and record downloaded file
                                    if self.state_manager and not dry_run and not file_output.error:
                                        self.state_manager.update_file_progress(
                                            supplier=supplier_name,
                                            file_index=idx,
                                            total_files=total_files
                                        )
                                        
                                        # Record file with brand and date for future duplicate detection
                                        # Use raw date strings from supplier (no conversion)
                                        valid_from_str = scraped_file.valid_from_date_str
                                        valid_to_str = scraped_file.expiry_date_str
                                        
                                        # If no valid_from date from API, try to detect from filename
                                        # Use original supplier filename if available (has the original date format)
                                        if not valid_from_str:
                                            detection_filename = scraped_file.supplier_filename or scraped_file.filename
                                            if detection_filename:
                                                detection_mode = scraper_config.get('schedule', {}).get('detection_mode', 'date_based')
                                                if detection_mode == 'date_based':
                                                    version = self.version_detector.detect_version(
                                                        item={'filename': detection_filename},
                                                        detection_mode=detection_mode
                                                    )
                                                    if version:
                                                        valid_from_str = version
                                                        logger.info(f"Detected date from filename for state tracking: {version}", filename=detection_filename)
                                        
                                        self.state_manager.add_downloaded_file(
                                            supplier=supplier_name,
                                            supplier_filename=scraped_file.supplier_filename or scraped_file.filename,
                                            valid_from_date=valid_from_str,
                                            drive_file_id=file_output.drive_file_id
                                        )
                            except Exception as e:
                                logger.error(f"Failed to process file {scraped_file.filename}: {str(e)}")
                                scraping_result.errors.append(f"File processing failed: {str(e)}")
            
            # Extract scraper URL for email summary
            scraper_url = None
            if 'links' in scraper_config and scraper_config['links'].get('page_url'):
                # link_downloader, custom (MATEROM), etc.
                scraper_url = scraper_config['links'].get('page_url')
            elif 'api' in scraper_config and scraper_config['api'].get('base_url'):
                # api_client (NEOPARTA)
                scraper_url = scraper_config['api'].get('base_url')
            elif 'authentication' in scraper_config and scraper_config['authentication'].get('login_url'):
                # Fallback: use login URL if no other URL available
                scraper_url = scraper_config['authentication'].get('login_url')
            
            return SupplierScrapingResult(
                supplier=supplier_name,
                scraping_result=scraping_result,
                files_processed=files_processed,
                scraper_url=scraper_url,
                total_files_found=scraping_result.total_files_found,
                files_skipped_duplicates=scraping_result.files_skipped_duplicates,
                errors=scraping_result.errors  # Transfer errors from scraper to result
            )
            
        except Exception as e:
            import traceback
            logger.error(f"Scraper failed for {supplier_name}: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise
    
    async def _process_scraped_file(
        self,
        scraped_file: ScrapedFile,
        scraper_config: Dict[str, Any],
        dry_run: bool,
        skip_bigquery: bool = False
    ) -> Optional[ScrapedFileOutput]:
        """
        Process a single scraped file through the parsing pipeline.
        
        Args:
            scraped_file: Scraped file to process
            scraper_config: Scraper configuration
            dry_run: Skip Drive upload if True
            skip_bigquery: Skip BigQuery upload and supersession reconciliation if True
            
        Returns:
            ScrapedFileOutput or None if processing failed
        """
        logger.info(f"Processing scraped_file: {scraped_file.filename}")
        
        # Get supplier's raw brand from scraped file (e.g., "VAG-OIL", "BMW_PART1")
        raw_supplier_brand = scraped_file.brand
        if not raw_supplier_brand:
            raw_supplier_brand = scraper_config.get('brand')
        
        if not raw_supplier_brand:
            logger.warning(f"No brand detected for {scraped_file.filename}")
            return ScrapedFileOutput(
                filename=scraped_file.filename,
                local_path=scraped_file.local_path,
                supplier=scraped_file.supplier,
                error="Brand detection failed"
            )
        
        # Extract config brand for parsing config lookup
        # (e.g., "VAG-OIL" -> "VAG", "BMW_PART1" -> "BMW")
        from scrapers.brand_matcher import extract_config_brand, normalize_brand
        config_brand = extract_config_brand(raw_supplier_brand, self.brand_configs)
        
        if not config_brand:
            logger.warning(f"No config brand found for supplier brand: {raw_supplier_brand}")
            return ScrapedFileOutput(
                filename=scraped_file.filename,
                local_path=scraped_file.local_path,
                brand=raw_supplier_brand,  # Use raw for error output
                supplier=scraped_file.supplier,
                error=f"No brand configuration for {raw_supplier_brand}"
            )
        
        # Get brand config using config brand
        # Note: For Drive upload, we may need to fall back to parent brand config
        brand_config = self.brand_config_map.get(config_brand.upper())
        if not brand_config:
            logger.error(f"Brand config lookup failed: {config_brand}")
            return ScrapedFileOutput(
                filename=scraped_file.filename,
                local_path=scraped_file.local_path,
                brand=raw_supplier_brand,
                supplier=scraped_file.supplier,
                error=f"No brand configuration for {config_brand}"
            )
        
        # For Drive folder lookup, we might need the parent brand's config
        # (e.g., FORD_OILS should use FORD's Drive folder)
        drive_brand_config = brand_config
        
        # Normalize supplier brand for filename generation
        # (e.g., "VAG-OIL" -> "VAG_OIL", "BMW_PART1" -> "BMW_PART1")
        normalized_brand = normalize_brand(raw_supplier_brand)
        
        logger.info(
            f"Brand mapping: raw='{raw_supplier_brand}' -> normalized='{normalized_brand}' -> config='{config_brand}'"
        )
        
        # Build supplier config from scraper_config (this function only processes web scrapers)
        # Web scrapers should NEVER check supplier_config_map (that's for email suppliers only)
        
        # Track which brand config we actually use (may differ if we fall back to parent brand)
        matched_config_brand = config_brand
        
        # Look for brand-specific config in scraper_config
        brand_configs_list = scraper_config.get('config', [])
        brand_specific_config = None
        search_brand = config_brand
        
        # Try to find config for this brand, and if not found, try parent brands
        # e.g., if BMW_FAST not found, try BMW
        while search_brand and not brand_specific_config:
            for config in brand_configs_list:
                if config.get('brand', '').upper() == search_brand.upper():
                    brand_specific_config = config
                    matched_config_brand = search_brand  # Update to the brand we actually found
                    break
            
            if brand_specific_config:
                # Found it!
                break
            
            # Not found, try parent brand by removing suffix
            last_sep_pos = max(search_brand.rfind('_'), search_brand.rfind('-'))
            if last_sep_pos > 0:
                search_brand = search_brand[:last_sep_pos]
                logger.info(f"Config not found for {config_brand}, trying parent brand: {search_brand}")
            else:
                # No more parents to try
                break
        
        if brand_specific_config:
            if matched_config_brand != config_brand:
                logger.info(f"Using parent brand {matched_config_brand} config for {config_brand}")
            else:
                logger.info(f"Using brand-specific config for {config_brand} from scraper config")
            
            supplier_config = {
                'supplier': scraped_file.supplier,
                'location': brand_specific_config.get('location', scraper_config.get('location', 'UNKNOWN')),
                'currency': brand_specific_config.get('currency', scraper_config.get('currency', 'EUR')),
                'decimalFormat': brand_specific_config.get('decimalFormat', scraper_config.get('decimalFormat')),
                'default_expiry_days': scraper_config.get('default_expiry_days', 90),
                'parsing_rules': {
                    'file_types': ['xlsx', 'xls', 'csv']
                },
                'config': [brand_specific_config]  # Include the brand-specific config
            }
        else:
            # No brand-specific config, create generic config (will use auto-detection)
            logger.info(f"No brand-specific config found for {config_brand}, creating generic config")
            supplier_config = {
                'supplier': scraped_file.supplier,
                'location': scraper_config.get('location', 'UNKNOWN'),
                'currency': scraper_config.get('currency', 'EUR'),
                'decimalFormat': scraper_config.get('decimalFormat'),
                'default_expiry_days': scraper_config.get('default_expiry_days', 90),
                'parsing_rules': {
                    'file_types': ['xlsx', 'xls', 'csv']
                }
            }
        
        # Parse dates
        expiry_date = scraped_file.expiry_date
        if not expiry_date:
            # Use default from config
            default_days = scraper_config.get('default_expiry_days', 90)
            expiry_date = self.date_parser.parse_expiry_date(
                email_body='',
                email_date=datetime.now(timezone.utc),
                default_days=default_days,
                system_default_days=self.core_config['defaults'].get('expiry_duration_days', 90)
            )[0]
        
        # Use valid_from_date if available (datetime), otherwise parse valid_from_date_str (string)
        valid_from_date = scraped_file.valid_from_date
        if not valid_from_date and scraped_file.valid_from_date_str:
            try:
                from datetime import datetime as dt_class
                # Parse ISO date string to datetime (e.g., "2025-11-05" -> datetime)
                valid_from_date = dt_class.fromisoformat(scraped_file.valid_from_date_str)
                if valid_from_date.tzinfo is None:
                    # Add UTC timezone if missing
                    valid_from_date = valid_from_date.replace(tzinfo=timezone.utc)
                logger.debug(f"Parsed valid_from_date_str '{scraped_file.valid_from_date_str}' to datetime: {valid_from_date}")
            except Exception as parse_error:
                logger.warning(f"Could not parse valid_from_date_str '{scraped_file.valid_from_date_str}': {parse_error}")
                valid_from_date = None
        
        # Fallback to current time if no valid date found
        if not valid_from_date:
            valid_from_date = datetime.now(timezone.utc)
            logger.warning(f"No valid_from_date found for {scraped_file.filename}, using current time (will cause duplicate detection issues)")
        
        logger.info(
            f"Processing {scraped_file.filename} - Supplier brand: {raw_supplier_brand}, Config brand: {config_brand}, "
            f"Valid from: {valid_from_date.isoformat()}, Expiry: {expiry_date.isoformat()}"
        )
        
        # Merge supplier brand config with brand config using centralized utility
        try:
            merged_brand_config = ConfigMerger.merge_supplier_brand_config(
                brand=matched_config_brand,  # Use matched config brand (may be parent brand like BMW)
                supplier_config=supplier_config,
                brand_config=brand_config
            )
            
            logger.info(
                f"Merged configuration",
                supplier_brand=raw_supplier_brand,  # Log both for clarity
                config_brand=config_brand,
                matched_config_brand=matched_config_brand,
                location=merged_brand_config.get('location'),
                currency=merged_brand_config.get('currency')
            )
        except (ValueError, TypeError) as e:
            error_msg = f"Config merge failed for {matched_config_brand}: {str(e)}"
            logger.error(error_msg)
            return ScrapedFileOutput(
                filename=scraped_file.filename,
                local_path=scraped_file.local_path,
                brand=raw_supplier_brand,  # Use supplier brand for output
                supplier=scraped_file.supplier,
                error=error_msg
            )
        
        # Generate CSV
        # If skip_bigquery is set, use simple streaming without BigQuery reconciliation
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                if skip_bigquery:
                    # Skip BigQuery - generate unreconciled CSV directly
                    logger.info("Skipping BigQuery reconciliation (--skip-bigquery flag)")
                    output_path, total_rows, valid_rows, warnings = \
                        self.file_generator.generate_csv_streaming(
                            input_file_path=scraped_file.local_path,
                            parser=self.parser,
                            brand_config=merged_brand_config,
                            supplier_config=supplier_config,
                            valid_from_date=valid_from_date,
                            output_path=temp_dir,
                            supplier_brand=normalized_brand,
                            matched_brand_text=raw_supplier_brand,
                            currency_detector=self.currency_detector
                        )
                    price_list_id = None
                    reconciliation_stats = None
                else:
                    # Normal flow - generate CSV with BigQuery supersession reconciliation
                    output_path, total_rows, valid_rows, warnings, recon_info = \
                        self.file_generator.generate_csv_with_bigquery_reconciliation(
                            input_file_path=scraped_file.local_path,
                            parser=self.parser,
                            brand_config=merged_brand_config,
                            supplier_config=supplier_config,
                            valid_from_date=valid_from_date,
                            output_path=temp_dir,
                            bq_processor=self.bq_processor,
                            supplier_brand=normalized_brand,
                            matched_brand_text=raw_supplier_brand,
                            currency_detector=self.currency_detector,
                            source_email_subject=None,  # Not applicable for web scrapers
                            source_email_date=None
                        )
                    price_list_id = recon_info.get('price_list_id')
                    reconciliation_stats = recon_info.get('stats')
                
                # Calculate parsing errors
                parsing_errors_count = total_rows - valid_rows
                
                logger.info(f"Generated CSV: {Path(output_path).name} ({valid_rows}/{total_rows} rows)")
                
                # Check if we got any valid rows
                if valid_rows == 0:
                    error_msg = f"Parsing failed: 0/{total_rows} valid rows. File not uploaded."
                    logger.error(error_msg)
                    logger.error(
                        f"Possible causes: header detection failed, all rows filtered out, or data format mismatch. "
                        f"Check file format and headers for {config_brand}"
                    )
                    
                    # Clean up the empty CSV file and source file
                    try:
                        if Path(output_path).exists():
                            Path(output_path).unlink()
                            logger.info(f"Deleted empty CSV file: {output_path}")
                    except Exception as cleanup_error:
                        logger.warning(f"Failed to cleanup empty CSV: {cleanup_error}")
                    
                    self._cleanup_source_file(scraped_file.local_path)
                    
                    return ScrapedFileOutput(
                        filename=scraped_file.filename,
                        local_path=scraped_file.local_path,
                        brand=raw_supplier_brand,
                        supplier=scraped_file.supplier,
                        warnings=warnings,
                        error=error_msg,
                        total_rows=total_rows,
                        valid_rows=0,
                        parsing_errors_count=total_rows,
                        price_list_id=price_list_id,
                        reconciliation_stats=reconciliation_stats
                    )
                
                # Upload to Drive (unless dry run)
                drive_file_id = None
                drive_link = None
                
                if not dry_run:
                    try:
                        # Get Drive folder ID - may need to use parent brand's folder
                        # (e.g., FORD_OILS uses FORD's Drive folder)
                        drive_folder_id = drive_brand_config.get('driveFolderId', '')
                        
                        # If no Drive folder in this brand's config, try parent brand
                        if not drive_folder_id and matched_config_brand != config_brand:
                            parent_brand_config = self.brand_config_map.get(matched_config_brand.upper())
                            if parent_brand_config:
                                drive_folder_id = parent_brand_config.get('driveFolderId', '')
                                drive_brand_config = parent_brand_config
                                logger.info(f"Using parent brand {matched_config_brand}'s Drive folder for {config_brand}")
                        
                        if not drive_folder_id:
                            raise ValueError(f"No Drive folder ID found for {config_brand} or {matched_config_brand}")
                        
                        # Use upload_file_with_archive to automatically archive old versions
                        upload_result = self.drive_uploader.upload_file_with_archive(
                            file_path=output_path,
                            folder_id=drive_folder_id,
                            brand=matched_config_brand,  # Use matched brand for Drive folder lookup
                            archive_old=True  # Always archive old versions
                        )
                        
                        drive_file_id = upload_result['file_id']
                        drive_link = upload_result['web_view_link']
                        
                        if 'warning' in upload_result:
                            warnings.append(upload_result['warning'])
                        
                        if 'archived_file_id' in upload_result:
                            logger.info(
                                f"Archived previous version and uploaded new file",
                                archived_id=upload_result['archived_file_id'],
                                new_link=drive_link
                            )
                        else:
                            logger.info(f"Uploaded to Drive: {drive_link}")
                        
                    except Exception as e:
                        error_msg = f"Drive upload failed: {str(e)}"
                        logger.error(error_msg)
                        return ScrapedFileOutput(
                            filename=Path(output_path).name,
                            local_path=output_path,
                            brand=raw_supplier_brand,  # Use supplier brand for output
                            supplier=scraped_file.supplier,
                            warnings=warnings,
                            error=error_msg,
                            total_rows=total_rows,
                            valid_rows=valid_rows,
                            parsing_errors_count=parsing_errors_count,
                            price_list_id=price_list_id,
                            reconciliation_stats=reconciliation_stats
                        )
                else:
                    logger.info("Dry run mode - skipping Drive upload")
                
                # Clean up source file after successful processing
                self._cleanup_source_file(scraped_file.local_path)
                
                return ScrapedFileOutput(
                    filename=Path(output_path).name,
                    local_path=output_path,
                    drive_file_id=drive_file_id,
                    drive_link=drive_link,
                    brand=raw_supplier_brand,  # Use supplier brand for output
                    supplier=scraped_file.supplier,
                    warnings=warnings,
                    total_rows=total_rows,
                    valid_rows=valid_rows,
                    parsing_errors_count=parsing_errors_count,
                    price_list_id=price_list_id,
                    reconciliation_stats=reconciliation_stats
                )
                
        except Exception as e:
            logger.error(f"Failed to generate CSV: {str(e)}")
            return ScrapedFileOutput(
                filename=scraped_file.filename,
                local_path=scraped_file.local_path,
                brand=raw_supplier_brand,  # Use supplier brand for output
                supplier=scraped_file.supplier,
                error=f"CSV generation failed: {str(e)}"
            )
    
    def _cleanup_source_file(self, file_path: str) -> None:
        """
        Clean up downloaded source file after successful processing.
        
        Args:
            file_path: Path to the source file to delete
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up source file: {file_path}")
        except Exception as e:
            # Don't fail the whole process if cleanup fails
            logger.warning(f"Failed to clean up source file {file_path}: {str(e)}")
    
    def _is_supplier_scheduled(
        self,
        scraper_config: Dict,
        force: bool = False
    ) -> bool:
        """
        Check if a supplier is scheduled to run based on its configuration.
        
        Args:
            scraper_config: Scraper configuration
            force: If True, ignore schedule and force execution
            
        Returns:
            True if supplier should run now
        """
        if not self.state_manager:
            # Fallback to simple logic if no state manager
            schedule = scraper_config.get('schedule', {})
            frequency = schedule.get('frequency', 'weekly')
            
            if frequency == 'daily':
                return True
            elif frequency == 'weekly':
                day_of_week: str = schedule.get('day_of_week', 'monday')
                current_day_name: str = datetime.now().strftime('%A').lower()
                return current_day_name == day_of_week.lower()
            elif frequency == 'monthly':
                day_of_month: int = int(schedule.get('day_of_month', 1))
                current_day_number: int = datetime.now().day
                return current_day_number == day_of_month
            return False
        
        # Use ScheduleEvaluator for sophisticated scheduling
        supplier_name = scraper_config['supplier']
        supplier_state = self.state_manager.get_supplier_state(supplier_name)
        
        return self.schedule_evaluator.should_run_scraper(
            scraper_config=scraper_config,
            supplier_state=supplier_state,
            current_time=datetime.now(timezone.utc),
            force=force
        )
    
    async def run_scraper_with_timeout(
        self,
        scraper_config: Dict,
        monitor: ExecutionMonitor,
        dry_run: bool = False,
        skip_bigquery: bool = False
    ) -> SupplierScrapingResult:
        """
        Run scraper with timeout monitoring and resume support.
        
        Args:
            scraper_config: Scraper configuration
            monitor: ExecutionMonitor for timeout tracking
            dry_run: Skip Drive upload if True
            skip_bigquery: Skip BigQuery upload and supersession reconciliation if True
            
        Returns:
            SupplierScrapingResult
        """
        supplier_name = scraper_config['supplier']
        
        # Get supplier state to check for interruptions
        supplier_state = {}
        start_index = 0
        
        if self.state_manager:
            # Clean up old file entries (older than 90 days) to prevent state file from growing indefinitely
            try:
                removed_count = self.state_manager.cleanup_old_files(
                    supplier=supplier_name,
                    retention_days=90
                )
                if removed_count > 0:
                    logger.info(
                        f"Cleaned up {removed_count} old file entries for {supplier_name}",
                        retention_days=90
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to cleanup old files for {supplier_name}: {str(e)}",
                    error=str(e)
                )
            
            # Get supplier state after cleanup
            supplier_state = self.state_manager.get_supplier_state(supplier_name)
            if supplier_state.get('interrupted', False):
                start_index = supplier_state.get('last_file_index', 0)
                logger.info(
                    f"Resuming interrupted scraper for {supplier_name}",
                    start_index=start_index
                )
        
        # Run the scraper
        result = await self._process_supplier_scraper(
            scraper_config=scraper_config,
            dry_run=dry_run,
            start_index=start_index,
            skip_bigquery=skip_bigquery
        )
        
        # Check if we should stop due to timeout
        if monitor.should_stop():
            if result.scraping_result.files:
                # Mark as interrupted
                if self.state_manager:
                    self.state_manager.mark_supplier_interrupted(
                        supplier_name,
                        len(result.scraping_result.files)
                    )
                
                warning_msg = (
                    f"Execution interrupted due to timeout - "
                    f"{len(result.scraping_result.files)} files processed"
                )
                result.warnings.append(warning_msg)
                logger.warning(warning_msg, supplier=supplier_name)
        else:
            # Completed successfully
            if self.state_manager:
                # Clear interrupted flag
                self.state_manager.clear_supplier_interrupted(supplier_name)
                
                # Update version if detectable
                if result.scraping_result.files:
                    version = self._detect_latest_version(
                        result.scraping_result.files,
                        scraper_config
                    )
                    if version:
                        supplier_state['last_version'] = version
                        self.state_manager.update_supplier_state(supplier_name, supplier_state)
                        logger.info(
                            f"Updated version for {supplier_name}",
                            version=version
                        )
        
        return result
    
    def _detect_latest_version(
        self,
        files: List[ScrapedFile],
        scraper_config: Dict
    ) -> Optional[str]:
        """
        Detect latest version from scraped files.
        
        Args:
            files: List of scraped files
            scraper_config: Scraper configuration
            
        Returns:
            Version string or None
        """
        detection_mode = scraper_config.get('schedule', {}).get('detection_mode', 'date_based')
        
        if detection_mode == 'full_scan':
            return None  # No version detection for full scan
        
        latest_version: Optional[str] = None
        
        for file in files:
            # Create item dict for version detection
            item = {
                'filename': file.filename,
                'modified': file.valid_from_date.isoformat() if file.valid_from_date else None,
                'date': file.expiry_date.isoformat() if file.expiry_date else None
            }
            
            version = self.version_detector.detect_version(item, detection_mode)
            if version:
                if not latest_version or self.version_detector.is_newer_version(version, latest_version):
                    latest_version = version
        
        return latest_version

