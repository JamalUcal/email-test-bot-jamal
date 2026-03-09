"""
Processing orchestrator - coordinates the full email-to-drive workflow.

Manages the complete pipeline:
1. Email detection and filtering
2. Brand and date detection
3. File parsing
4. CSV generation (with optional BigQuery supersession reconciliation)
5. Google Drive upload
6. Summary reporting
"""

from typing import Dict, List, Optional, Any, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime, timezone
import tempfile
from pathlib import Path

from gmail.gmail_client import GmailClient
from gmail.email_processor import EmailProcessor, EmailResult
from gmail.attachment_handler import AttachmentHandler, Attachment
from parsers.brand_detector import BrandDetector
from parsers.currency_detector import CurrencyDetector
from parsers.date_parser import DateParser
from parsers.price_list_parser import PriceListParser
from parsers.header_detector import DetectedHeaders
from output.file_generator import FileGenerator
from output.drive_uploader import DriveUploader
from utils.logger import get_logger
from utils.exceptions import EmailProcessingError
from utils.config_merger import ConfigMerger

if TYPE_CHECKING:
    from storage.bigquery_processor import BigQueryPriceListProcessor

logger = get_logger(__name__)


@dataclass
class FileOutput:
    """Result of file generation and upload."""
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
class EmailProcessingResult:
    """Complete result of processing one email."""
    email_result: EmailResult
    brand_detected: Optional[str] = None
    brand_source: Optional[str] = None
    brand_fallback_used: bool = False
    expiry_date: Optional[datetime] = None
    expiry_source: Optional[str] = None
    expiry_is_past: bool = False
    valid_from_date: Optional[datetime] = None
    files_generated: List[FileOutput] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class ProcessingOrchestrator:
    """Orchestrates the complete email processing workflow."""
    
    def __init__(
        self,
        gmail_client: GmailClient,
        supplier_configs: List[Dict],
        brand_configs: List[Dict],
        core_config: Dict,
        service_account_info: Dict,
        column_mapping_config: Dict,
        currency_config: Dict
    ):
        """
        Initialize orchestrator.
        
        Args:
            gmail_client: GmailClient instance
            supplier_configs: List of supplier configurations
            brand_configs: List of brand configurations
            core_config: Core configuration
            service_account_info: Service account credentials
            column_mapping_config: Column mapping configuration for header detection (REQUIRED)
            currency_config: Currency configuration for currency detection (REQUIRED)
        """
        self.gmail_client = gmail_client
        self.supplier_configs = supplier_configs
        self.brand_configs = brand_configs
        self.core_config = core_config
        self.service_account_info = service_account_info
        
        # Initialize components
        self.email_processor = EmailProcessor(
            gmail_client=gmail_client,
            supplier_configs=supplier_configs,
            bucket_name=core_config['gcp']['bucket_name'],
            column_mapping_config=column_mapping_config,
            ignore_domains=core_config['gmail'].get('ignore_domains', []),
            ignore_email_patterns=core_config['gmail'].get('ignore_email_patterns', []),
            enable_parsing=False,  # We'll handle parsing ourselves
            currency_config=currency_config
        )
        
        self.attachment_handler = AttachmentHandler(gmail_client)
        self.brand_detector = BrandDetector(brand_configs)
        self.currency_detector = CurrencyDetector(currency_config)
        self.date_parser = DateParser(timezone=core_config['execution'].get('timezone', 'UTC'))
        self.parser = PriceListParser(
            column_mapping_config=column_mapping_config,
            currency_config=currency_config
        )
        self.file_generator = FileGenerator(column_mapping_config=column_mapping_config)
        
        # Initialize Drive uploader with delegated credentials
        drive_impersonation_email = core_config.get('drive', {}).get('impersonation_email')
        self.drive_uploader = DriveUploader(
            service_account_info=service_account_info,
            delegated_user=drive_impersonation_email
        )
        
        # Initialize BigQuery processor (required for supersession reconciliation)
        from storage.bigquery_processor import BigQueryPriceListProcessor
        bigquery_config = core_config['bigquery']
        self.bq_processor: 'BigQueryPriceListProcessor' = BigQueryPriceListProcessor(
            project_id=bigquery_config.get('project_id', core_config['gcp']['project_id']),
            dataset_id=bigquery_config.get('dataset_id', 'PRICING'),
            staging_bucket=bigquery_config.get('staging_bucket', core_config['gcp']['bucket_name']),
            service_account_info=service_account_info,
            bigquery_config=bigquery_config
        )
        
        # Create brand config lookup
        self.brand_config_map = {
            config['brand'].upper(): config
            for config in brand_configs
        }
        
        # Create supplier config lookup
        self.supplier_config_map = {
            config['supplier']: config
            for config in supplier_configs
        }
        
        logger.info(
            "ProcessingOrchestrator initialized",
            suppliers=len(supplier_configs),
            brands=len(brand_configs)
        )
    
    def process_email(
        self,
        message: Dict,
        dry_run: bool = False,
        skip_bigquery: bool = False
    ) -> EmailProcessingResult:
        """
        Process a single email through the complete workflow.
        
        Args:
            message: Gmail message object
            dry_run: If True, skip Drive upload
            skip_bigquery: If True, skip BigQuery upload and supersession reconciliation
            
        Returns:
            EmailProcessingResult with all processing details
        """
        # Step 1: Basic email processing (detection, filtering)
        email_result = self.email_processor.process_email(message)
        
        result = EmailProcessingResult(email_result=email_result)
        
        # Skip if ignored or unknown domain
        if email_result.is_ignored or email_result.is_unknown_domain:
            return result
        
        # Skip if no supplier match
        if not email_result.supplier_name:
            return result
        
        # Get supplier config
        supplier_config = self.supplier_config_map.get(email_result.supplier_name)
        if not supplier_config:
            result.errors.append(f"No config found for supplier: {email_result.supplier_name}")
            return result
        
        # Get email body for brand/date detection
        try:
            email_body = self.gmail_client.get_message_body(message)
        except Exception as e:
            logger.warning(f"Failed to get email body: {str(e)}")
            email_body = None
        
        # Process each supported attachment
        for attachment in email_result.supported_attachments:
            try:
                file_output = self._process_attachment(
                    attachment=attachment,
                    email_result=email_result,
                    supplier_config=supplier_config,
                    email_body=email_body,
                    dry_run=dry_run,
                    skip_bigquery=skip_bigquery
                )
                
                if file_output:
                    result.files_generated.append(file_output)
                    
                    # Update brand/date info from first successful file
                    if not result.brand_detected and file_output.brand:
                        result.brand_detected = file_output.brand
                
            except Exception as e:
                error_msg = f"Failed to process {attachment.filename}: {str(e)}"
                logger.error(error_msg, attachment=attachment.filename, error=str(e))
                result.errors.append(error_msg)
        
        return result
    
    def _process_attachment(
        self,
        attachment: Attachment,
        email_result: EmailResult,
        supplier_config: Dict[str, Any],
        email_body: Optional[str],
        dry_run: bool,
        skip_bigquery: bool = False
    ) -> Optional[FileOutput]:
        """
        Process a single attachment through the workflow.
        
        Args:
            attachment: Attachment to process
            email_result: Email result with metadata
            supplier_config: Supplier configuration
            email_body: Email body text
            dry_run: Skip Drive upload if True
            skip_bigquery: Skip BigQuery upload and supersession reconciliation if True
            
        Returns:
            FileOutput or None if processing failed
        """
        logger.info(f"Processing attachment: {attachment.filename}")
        
        # Step 1: Detect brand
        # If this is a forwarded email, try to use the original subject line for brand detection
        subject_for_detection = email_result.subject
        if email_result.detection_method == "forwarded" and email_body:
            forwarded_subject = self.gmail_client.parse_forwarded_subject(email_body)
            if forwarded_subject:
                logger.info(f"Using forwarded subject for brand detection: {forwarded_subject}")
                subject_for_detection = forwarded_subject
        
        config_brand, matched_brand_text, brand_source, brand_fallback = self.brand_detector.detect_brand(
            filename=attachment.filename,
            subject=subject_for_detection,
            body=email_body,
            default_brand=supplier_config.get('default_brand')
        )
        
        if not config_brand:
            logger.warning(f"Could not detect brand for {attachment.filename}")
            return FileOutput(
                filename=attachment.filename,
                local_path='',
                error="Brand detection failed"
            )
        
        logger.info(
            f"Brand detected: {config_brand} (matched: '{matched_brand_text}', source: {brand_source})"
        )
        
        # Check if brand should be ignored for this supplier
        ignore_brands = supplier_config.get('ignore_brands', [])
        if config_brand and config_brand.upper() in [b.upper() for b in ignore_brands]:
            logger.info(
                f"Skipping {attachment.filename}: brand '{config_brand}' is in ignore_brands for supplier",
                filename=attachment.filename,
                brand=config_brand,
                supplier=supplier_config.get('supplier')
            )
            return None  # Skip without error
        
        # Get brand config using canonical brand name
        brand_config = self.brand_config_map.get(config_brand.upper())
        if not brand_config:
            logger.warning(f"No brand config found for: {config_brand}")
            return FileOutput(
                filename=attachment.filename,
                local_path='',
                brand=config_brand,
                error=f"No brand configuration for {config_brand}"
            )
        
        # Step 1.5: Detect currency (5-layer hierarchy)
        detected_currency: Optional[str] = None
        
        # Layer 1: Check email body for CURRENCY: tag
        detected_currency = self.currency_detector.detect_currency_from_tag(email_body)
        if detected_currency:
            logger.info(f"Currency detected from email tag: {detected_currency}")
        
        if not detected_currency:
            # Layer 2: Check if supplier config is ambiguous
            if self.currency_detector.is_currency_ambiguous(supplier_config, config_brand):
                logger.info(f"Currency ambiguous for {config_brand}, proceeding to detection")
                
                # Layer 3: Check subject line and filename (scoped to supplier currencies)
                supplier_currencies = self.currency_detector.get_supplier_currencies_for_brand(
                    supplier_config, config_brand
                )
                detected_currency = self.currency_detector.detect_currency_from_text_scoped(
                    f"{subject_for_detection} {attachment.filename}",
                    allowed_currencies=supplier_currencies
                )
                if detected_currency:
                    logger.info(f"Currency detected from subject/filename: {detected_currency}")
            else:
                # Only 1 currency in config, will use it (existing behavior)
                logger.debug(f"Single currency in config for {config_brand}, will use config value")
                detected_currency = None  # Will use config value
        
        # Cache for detected headers from peek (to avoid re-parsing)
        cached_detected_headers: Optional[DetectedHeaders] = None
        
        # If still ambiguous and no currency detected, we need to peek the file
        if (not detected_currency and 
            self.currency_detector.is_currency_ambiguous(supplier_config, config_brand)):
            
            # Need to download file first to peek it
            try:
                file_paths = self.attachment_handler.download_attachments([attachment])
                file_path = file_paths[0]
            except Exception as e:
                logger.error(f"Failed to download attachment for currency detection: {str(e)}")
                return FileOutput(
                    filename=attachment.filename,
                    local_path='',
                    brand=config_brand,
                    error=f"Download failed: {str(e)}"
                )
            
            # Layer 4: Peek file content (also captures detected headers for reuse)
            detected_currency, cached_detected_headers = self.parser.peek_file_for_currency(
                file_path=file_path,
                currency_detector=self.currency_detector,
                allowed_currencies=supplier_currencies
            )
            if detected_currency:
                logger.info(f"Currency detected from file content: {detected_currency}")
            else:
                # Layer 5: FAIL - ambiguous and couldn't detect
                error_msg = (
                    f"Currency ambiguous: Supplier {supplier_config.get('supplier')} "
                    f"has multiple currencies for brand {config_brand} and currency could not be detected"
                )
                logger.error(error_msg)
                return FileOutput(
                    filename=attachment.filename,
                    local_path='',
                    brand=config_brand,
                    error=error_msg
                )
        
        # Merge supplier brand config with brand config (with optional currency override)
        try:
            merged_brand_config = ConfigMerger.merge_supplier_brand_config(
                brand=config_brand,
                supplier_config=supplier_config,
                brand_config=brand_config,
                override_currency=detected_currency
            )
        except ValueError as e:
            logger.warning(f"Config merge failed: {str(e)}")
            return FileOutput(
                filename=attachment.filename,
                local_path='',
                brand=config_brand,
                error=f"Configuration error: {str(e)}"
            )
        except TypeError as e:
            logger.error(f"Config validation failed: {str(e)}")
            return FileOutput(
                filename=attachment.filename,
                local_path='',
                brand=config_brand,
                error=f"Invalid configuration: {str(e)}"
            )
        
        # Step 2: Parse dates
        expiry_date, expiry_source, expiry_is_past = self.date_parser.parse_expiry_date(
            email_body=email_body or '',
            email_date=email_result.date,
            default_days=supplier_config.get('default_expiry_days'),
            system_default_days=self.core_config['defaults'].get('expiry_duration_days', 90)
        )
        
        valid_from_date = self.date_parser.parse_valid_from_date(
            email_body=email_body or '',
            email_date=email_result.date
        )
        
        logger.info(
            f"Dates parsed - Valid from: {valid_from_date.isoformat()}, "
            f"Expiry: {expiry_date.isoformat()} (source: {expiry_source})"
        )
        
        # Step 3: Download attachment (if not already downloaded for currency detection)
        if 'file_path' not in locals():
            try:
                file_paths = self.attachment_handler.download_attachments([attachment])
                file_path = file_paths[0]
            except Exception as e:
                logger.error(f"Failed to download attachment: {str(e)}")
                return FileOutput(
                    filename=attachment.filename,
                    local_path='',
                    brand=config_brand,
                    error=f"Download failed: {str(e)}"
                )
        
        # Step 4 & 5: Stream parse and generate CSV
        # If skip_bigquery is set, use simple streaming without BigQuery reconciliation
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                if skip_bigquery:
                    # Skip BigQuery - generate unreconciled CSV directly
                    logger.info("Skipping BigQuery reconciliation (--skip-bigquery flag)")
                    output_path, total_rows, valid_rows, warnings = \
                        self.file_generator.generate_csv_streaming(
                            input_file_path=file_path,
                            parser=self.parser,
                            brand_config=merged_brand_config,
                            supplier_config=supplier_config,
                            valid_from_date=valid_from_date,
                            output_path=temp_dir,
                            matched_brand_text=matched_brand_text,
                            currency_detector=self.currency_detector,
                            detected_headers=cached_detected_headers
                        )
                    price_list_id = None
                    reconciliation_stats = None
                else:
                    # Normal flow - generate CSV with BigQuery supersession reconciliation
                    output_path, total_rows, valid_rows, warnings, recon_info = \
                        self.file_generator.generate_csv_with_bigquery_reconciliation(
                            input_file_path=file_path,
                            parser=self.parser,
                            brand_config=merged_brand_config,
                            supplier_config=supplier_config,
                            valid_from_date=valid_from_date,
                            output_path=temp_dir,
                            bq_processor=self.bq_processor,
                            matched_brand_text=matched_brand_text,
                            currency_detector=self.currency_detector,
                            detected_headers=cached_detected_headers,
                            source_email_subject=email_result.subject,
                            source_email_date=email_result.date
                        )
                    price_list_id = recon_info.get('price_list_id')
                    reconciliation_stats = recon_info.get('stats')
                
                # Calculate parsing errors
                parsing_errors_count = total_rows - valid_rows
                
                logger.info(f"Streamed {valid_rows}/{total_rows} items from {attachment.filename} to CSV")
                
                logger.info(f"Generated CSV: {Path(output_path).name}")
                
                # Step 6: Upload to Drive (unless dry run)
                drive_file_id = None
                drive_link = None
                
                if not dry_run:
                    try:
                        upload_result = self.drive_uploader.upload_file(
                            file_path=output_path,
                            folder_id=brand_config['driveFolderId'],
                            brand=config_brand
                        )
                        
                        drive_file_id = upload_result['file_id']
                        drive_link = upload_result['web_view_link']
                        
                        if 'warning' in upload_result:
                            warnings.append(upload_result['warning'])
                        
                        logger.info(f"Uploaded to Drive: {drive_link}")
                        
                        # Update BigQuery record with Drive info
                        if price_list_id:
                            try:
                                self.bq_processor.update_drive_info(
                                    price_list_id=price_list_id,
                                    drive_file_id=drive_file_id,
                                    drive_file_url=drive_link
                                )
                            except Exception as e:
                                logger.warning(f"Failed to update BigQuery with Drive info: {e}")
                        
                        # Cleanup GCS staging files after successful Drive upload
                        if price_list_id:
                            try:
                                self.bq_processor.cleanup_gcs_files(price_list_id)
                            except Exception as e:
                                logger.warning(f"Failed to cleanup GCS files: {e}")
                        
                    except Exception as e:
                        error_msg = f"Drive upload failed: {str(e)}"
                        logger.error(error_msg)
                        return FileOutput(
                            filename=Path(output_path).name,
                            local_path=output_path,
                            brand=config_brand,
                            supplier=email_result.supplier_name,
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
                
                return FileOutput(
                    filename=Path(output_path).name,
                    local_path=output_path,
                    drive_file_id=drive_file_id,
                    drive_link=drive_link,
                    brand=config_brand,
                    supplier=email_result.supplier_name,
                    warnings=warnings,
                    total_rows=total_rows,
                    valid_rows=valid_rows,
                    parsing_errors_count=parsing_errors_count,
                    price_list_id=price_list_id,
                    reconciliation_stats=reconciliation_stats
                )
                
        except Exception as e:
            logger.error(f"Failed to generate CSV: {str(e)}")
            return FileOutput(
                filename=attachment.filename,
                local_path='',
                brand=config_brand,
                error=f"CSV generation failed: {str(e)}"
            )
    
    def process_emails(
        self,
        after_date: Optional[datetime] = None,
        before_date: Optional[datetime] = None,
        max_emails: int = 100,
        dry_run: bool = False,
        skip_bigquery: bool = False
    ) -> tuple[List[EmailProcessingResult], Optional[datetime]]:
        """
        Process multiple emails from mailbox.
        
        Retries up to 10 times with 1-week increments if no emails found.
        
        Args:
            after_date: Only process emails after this date
            before_date: Only process emails before this date (overrides default 7-day window)
            max_emails: Maximum number of emails to process
            dry_run: Skip Drive upload if True
            skip_bigquery: Skip BigQuery upload and supersession reconciliation if True
            
        Returns:
            Tuple of (List of EmailProcessingResult, final_search_date)
            final_search_date is the end of the search window (for updating state)
        """
        results = []
        current_date = after_date
        original_after_date = after_date  # Preserve original for filtering
        max_retries = 10
        retry_count = 0
        
        try:
            # Build query
            query = 'to:pricing-bot@ucalexports.com'
            logger.info(f"Gmail query: {query}")
            
            # Retry up to 10 times with 1-week increments
            while retry_count < max_retries:
                logger.info(
                    f"Search attempt {retry_count + 1}/{max_retries}",
                    current_date=current_date.isoformat() if current_date else None
                )
                
                # Fetch ALL emails in date range (no limit on API query)
                # We'll sort and limit after fetching to ensure chronological order
                messages = self.gmail_client.list_messages(
                    query=query,
                    after_date=current_date,
                    before_date=before_date
                )
                
                logger.info(f"📬 Fetched {len(messages)} emails", count=len(messages))
                
                # Sort messages by date (oldest first) and filter by exact timestamp
                messages_with_dates = []
                for msg_metadata in messages:
                    message = self.gmail_client.get_message(msg_metadata['id'])
                    date = self.gmail_client.get_message_date(message)
                    
                    # Filter out emails at or before the ORIGINAL after_date (avoid duplicates)
                    # Use <= to exclude already-processed emails (state stores last processed timestamp)
                    if original_after_date and date and date <= original_after_date:
                        logger.info(
                            f"Skipping email (already processed)",
                            message_id=message['id'],
                            email_date=date.isoformat(),
                            cutoff_date=original_after_date.isoformat()
                        )
                        continue
                    
                    messages_with_dates.append((date, message))
                
                # Sort by date (oldest first)
                messages_with_dates.sort(key=lambda x: x[0] if x[0] else datetime.min)
                
                logger.info(
                    f"Filtered {len(messages_with_dates)} emails (from {len(messages)} fetched)",
                    filtered_count=len(messages_with_dates),
                    fetched_count=len(messages)
                )
                
                # If we found emails, process them and break
                if messages_with_dates:
                    # Limit to max_emails AFTER sorting to ensure we process oldest first
                    messages_to_process = messages_with_dates[:max_emails]
                    
                    logger.info(
                        f"✅ Found {len(messages_with_dates)} emails, will process {len(messages_to_process)} (limit: {max_emails})",
                        total_found=len(messages_with_dates),
                        to_process=len(messages_to_process),
                        max_emails=max_emails
                    )
                    
                    # Process each message
                    for date, message in messages_to_process:
                        result = self.process_email(message, dry_run=dry_run, skip_bigquery=skip_bigquery)
                        results.append(result)
                    
                    # Update current_date to the newest processed email's timestamp
                    # This ensures the state is advanced past all processed emails
                    # messages_to_process is sorted oldest-first, so last entry is newest
                    newest_email_date = messages_to_process[-1][0]
                    if newest_email_date:
                        current_date = newest_email_date
                        logger.info(
                            f"📅 Updated checkpoint to newest processed email: {current_date.isoformat()}",
                            newest_email_date=current_date.isoformat()
                        )
                    
                    logger.info(f"✅ Processing complete: {len(results)} emails processed")
                    break
                else:
                    # No emails found, increment by 1 week and retry
                    retry_count += 1
                    if retry_count < max_retries:
                        from datetime import timedelta
                        now = datetime.now(timezone.utc)
                        next_date = current_date + timedelta(weeks=1) if current_date else now
                        # Stop if we've reached or passed the current date
                        if next_date >= now:
                            logger.info(
                                f"⏹️ Reached current date, stopping search",
                                current_date=current_date.isoformat() if current_date else None,
                                now=now.isoformat()
                            )
                            break
                        current_date = next_date
                        logger.info(
                            f"⏭️ No emails found, advancing 1 week to {current_date.isoformat()}",
                            retry=retry_count,
                            new_date=current_date.isoformat()
                        )
                    else:
                        logger.warning(
                            f"⚠️ No emails found after {max_retries} attempts (searched up to {current_date.isoformat() if current_date else 'unknown'})",
                            retries=max_retries,
                            final_date=current_date.isoformat() if current_date else None
                        )
            
        except Exception as e:
            logger.error(f"Failed to process emails: {str(e)}", error=str(e))
        
        # Return results and the final search date (end of last search window)
        # This ensures we don't re-search the same empty windows
        return results, current_date
    
    def cleanup(self):
        """Clean up resources."""
        self.attachment_handler.cleanup()
