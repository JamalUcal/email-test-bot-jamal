"""
Email processor - coordinates email detection and filtering.

Filters emails by supplier domain and manages the processing workflow.
"""

from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .gmail_client import GmailClient
from .attachment_handler import AttachmentHandler, Attachment
from parsers.price_list_parser import PriceListParser
from storage.pricing_storage import PricingStorage
from utils.logger import get_logger
from utils.exceptions import EmailError, ParsingError

logger = get_logger(__name__)


@dataclass
class EmailResult:
    """Result of processing a single email."""
    message_id: str
    from_address: str
    from_domain: str
    subject: str
    date: datetime
    supplier_name: Optional[str] = None
    attachments: List[Attachment] = field(default_factory=list)
    supported_attachments: List[Attachment] = field(default_factory=list)
    warning_attachments: List[Attachment] = field(default_factory=list)
    ignored_attachments: List[Attachment] = field(default_factory=list)
    error: Optional[str] = None
    is_unknown_domain: bool = False
    is_ignored: bool = False
    parsed_attachments: int = 0
    items_extracted: int = 0
    parsing_errors: List[str] = field(default_factory=list)
    scraper_url: Optional[str] = None  # URL for web scraper sources
    scraper_all_duplicates: bool = False  # True if scraper found files but all were duplicates
    detection_method: Optional[str] = None  # Method used to detect supplier: "body_tag", "forwarded", "direct", "unknown"
    original_sender: Optional[str] = None  # Original sender email for forwarded messages


@dataclass
class ProcessingResults:
    """Results of processing all emails."""
    emails_processed: int = 0
    emails_from_suppliers: int = 0
    emails_unknown_domain: int = 0
    emails_ignored: int = 0
    total_attachments: int = 0
    supported_attachments: int = 0
    warning_attachments: int = 0
    results: List[EmailResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class EmailProcessor:
    """Processes emails with supplier domain filtering."""
    
    def __init__(
        self,
        gmail_client: GmailClient,
        supplier_configs: List[Dict],
        bucket_name: str,
        column_mapping_config: Dict,
        ignore_domains: Optional[List[str]] = None,
        ignore_email_patterns: Optional[List[str]] = None,
        enable_parsing: bool = True,
        currency_config: Optional[Dict] = None
    ):
        """
        Initialize email processor.
        
        Args:
            gmail_client: GmailClient instance
            supplier_configs: List of supplier configurations
            bucket_name: GCS bucket name for storing parsed data
            column_mapping_config: Column mapping configuration for header detection
            ignore_domains: List of domains to ignore (e.g., ["ucalexports.com"])
            ignore_email_patterns: List of email patterns to ignore (e.g., ["noreply@", "no-reply@"])
            enable_parsing: Whether to parse attachments (default True)
            currency_config: Currency configuration for price prefix stripping (optional)
        """
        self.gmail_client = gmail_client
        self.attachment_handler = AttachmentHandler(gmail_client)
        self.supplier_configs = supplier_configs  # Store full configs for parsing
        self.enable_parsing = enable_parsing
        self.ignore_domains = set(domain.lower() for domain in (ignore_domains or []))
        self.ignore_email_patterns = [pattern.lower() for pattern in (ignore_email_patterns or [])]
        
        # Initialize parser and storage if parsing is enabled
        self.parser: Optional[PriceListParser]
        self.storage: Optional[PricingStorage]
        if self.enable_parsing:
            self.parser = PriceListParser(
                column_mapping_config=column_mapping_config,
                currency_config=currency_config
            )
            self.storage = PricingStorage(bucket_name)
        else:
            self.parser = None
            self.storage = None
        
        # Build domain to supplier mapping AND email address to supplier mapping
        self.domain_to_supplier: Dict[str, str] = {}
        self.email_to_supplier: Dict[str, str] = {}  # Exact email address matching
        self.supplier_config_map: Dict[str, Dict] = {}  # Map supplier name to full config
        
        for config in supplier_configs:
            supplier_name = config['supplier']
            self.supplier_config_map[supplier_name] = config
            
            # Map by domain (if provided) - support both single string and array
            if 'email_domain' in config and config['email_domain']:
                email_domain = config['email_domain']
                if isinstance(email_domain, list):
                    for domain in email_domain:
                        if domain:
                            self.domain_to_supplier[domain.lower()] = supplier_name
                else:
                    self.domain_to_supplier[email_domain.lower()] = supplier_name
            
            # Map by exact email address(es) - support both single and multiple
            # Single email_address (backwards compatibility)
            if 'email_address' in config and config['email_address']:
                email = config['email_address'].lower()
                self.email_to_supplier[email] = supplier_name
            
            # Multiple email_addresses (array)
            if 'email_addresses' in config and config['email_addresses']:
                for email in config['email_addresses']:
                    if email:  # Skip empty strings
                        self.email_to_supplier[email.lower()] = supplier_name
        
        logger.info(
            "Email processor initialized",
            supplier_count_by_domain=len(self.domain_to_supplier),
            email_addresses_mapped=len(self.email_to_supplier),
            total_suppliers=len(self.supplier_config_map),
            ignore_domains=list(self.ignore_domains),
            ignore_patterns=self.ignore_email_patterns,
            parsing_enabled=self.enable_parsing
        )
    
    def should_ignore_email(self, from_address: str, from_domain: str) -> bool:
        """
        Check if email should be ignored.
        
        Args:
            from_address: Email address
            from_domain: Email domain
            
        Returns:
            True if email should be ignored
        """
        from_address_lower = from_address.lower()
        from_domain_lower = from_domain.lower()
        
        # Check if domain is in ignore list
        if from_domain_lower in self.ignore_domains:
            return True
        
        # Check if email matches any ignore patterns
        for pattern in self.ignore_email_patterns:
            if from_address_lower.startswith(pattern):
                return True
        
        return False
    
    def get_supplier_for_email(self, email_address: str) -> Optional[str]:
        """
        Get supplier name for exact email address.
        
        Args:
            email_address: Full email address
            
        Returns:
            Supplier name or None if not found
        """
        return self.email_to_supplier.get(email_address.lower())
    
    def get_supplier_for_domain(self, domain: str) -> Optional[str]:
        """
        Get supplier name for domain.
        
        Args:
            domain: Email domain
            
        Returns:
            Supplier name or None if not found
        """
        return self.domain_to_supplier.get(domain.lower())
    
    def get_supplier_from_address(self, email_address: str) -> Optional[str]:
        """
        Get supplier name from email address.
        
        Tries exact email match first, then falls back to domain matching.
        
        Args:
            email_address: Full email address
            
        Returns:
            Supplier name or None if not found
        """
        # Try exact email match first (higher priority)
        supplier = self.get_supplier_for_email(email_address)
        if supplier:
            return supplier
        
        # Fall back to domain matching
        domain = self.gmail_client.get_domain(email_address)
        return self.get_supplier_for_domain(domain)
    
    def detect_supplier_from_message(
        self,
        message: Dict,
        from_header: str
    ) -> tuple[str, str, Optional[str], str, Optional[str]]:
        """
        Detect supplier using three-layer detection strategy.
        
        Layer 1: Body tag - SUPPLIER: <name>
        Layer 2: Forwarded email parsing - extract original sender
        Layer 3: Direct From header (fallback)
        
        Args:
            message: Gmail message object
            from_header: From header value
            
        Returns:
            Tuple of (from_address, from_domain, supplier_name, detection_method, original_sender)
        """
        # Extract body for parsing
        body = self.gmail_client.get_message_body(message)
        
        # Default values from From header
        from_address = self.gmail_client.extract_email_address(from_header)
        from_domain = self.gmail_client.get_domain(from_address)
        supplier_name: Optional[str] = None
        detection_method = "unknown"
        original_sender: Optional[str] = None
        
        # Layer 1: Check for SUPPLIER: tag in body
        if body:
            tagged_supplier = self.gmail_client.parse_supplier_tag(body)
            if tagged_supplier:
                # Validate against known suppliers (case-insensitive)
                for config in self.supplier_configs:
                    if config['supplier'].upper() == tagged_supplier.upper():
                        supplier_name = config['supplier']
                        detection_method = "body_tag"
                        logger.info(
                            f"✓ Supplier detected via BODY TAG: {supplier_name}",
                            supplier=supplier_name,
                            method="body_tag"
                        )
                        return from_address, from_domain, supplier_name, detection_method, original_sender
                
                # Tag found but not in configs
                logger.warning(
                    f"⚠️ SUPPLIER tag found ({tagged_supplier}) but not in supplier configs",
                    tagged_supplier=tagged_supplier
                )
        
        # Layer 2: Check for forwarded email
        if body:
            forwarded_email = self.gmail_client.parse_forwarded_email(body)
            if forwarded_email:
                original_sender = forwarded_email
                # Try exact email match first, then domain
                supplier_name = self.get_supplier_from_address(forwarded_email)
                if supplier_name:
                    detection_method = "forwarded"
                    logger.info(
                        f"✓ Supplier detected via FORWARDED EMAIL: {supplier_name}",
                        supplier=supplier_name,
                        original_sender=original_sender,
                        forwarded_email=forwarded_email,
                        method="forwarded"
                    )
                    return from_address, from_domain, supplier_name, detection_method, original_sender
                else:
                    # Forwarded email found but not recognized
                    logger.info(
                        f"ℹ️ Forwarded email detected but not recognized: {forwarded_email}",
                        forwarded_email=forwarded_email
                    )
        
        # Layer 3: Direct From header (fallback)
        # Try exact email match first, then domain
        supplier_name = self.get_supplier_from_address(from_address)
        if supplier_name:
            detection_method = "direct"
            logger.info(
                f"✓ Supplier detected via DIRECT FROM HEADER: {supplier_name}",
                supplier=supplier_name,
                from_address=from_address,
                from_domain=from_domain,
                method="direct"
            )
            return from_address, from_domain, supplier_name, detection_method, original_sender

        # Layer 4: Subject/body hint fallback (when Layers 1-3 did not find a supplier)
        # Hints map: (search_phrase, ...) -> config supplier name
        subject_body_hints: Dict[str, tuple] = {
            "GTAUTO": ("GT AUTO", "GTAUTO"),
            "TECHNOPARTS": ("TECHNOPARTS",),
        }
        subject = (self.gmail_client.get_header(message, "subject") or "").strip()
        combined_text = f"{subject}\n{body or ''}".upper()
        for config_supplier, hints in subject_body_hints.items():
            for hint in hints:
                if hint.upper() in combined_text:
                    # Validate against known suppliers
                    for config in self.supplier_configs:
                        if config["supplier"].upper() == config_supplier.upper():
                            supplier_name = config["supplier"]
                            detection_method = "subject_hint" if hint.upper() in subject.upper() else "body_hint"
                            logger.info(
                                f"✓ Supplier detected via SUBJECT/BODY HINT: {supplier_name} (matched '{hint}')",
                                supplier=supplier_name,
                                hint=hint,
                                method=detection_method,
                            )
                            return from_address, from_domain, supplier_name, detection_method, original_sender
                    break

        detection_method = "unknown"
        logger.info(
            f"❌ No supplier detected for address/domain: {from_address} ({from_domain})",
            from_address=from_address,
            from_domain=from_domain,
            method="unknown"
        )
        return from_address, from_domain, supplier_name, detection_method, original_sender
    
    def _parse_attachments(
        self,
        attachments: List[Attachment],
        supplier_name: str,
        message_id: str,
        email_date: datetime
    ) -> tuple[int, int, List[str]]:
        """
        Parse attachments and store pricing data.
        
        Args:
            attachments: List of attachments to parse
            supplier_name: Supplier name
            message_id: Gmail message ID
            email_date: Email date
            
        Returns:
            Tuple of (parsed_count, items_extracted, errors)
        """
        # Guard clause: ensure parser and storage are available
        if self.parser is None or self.storage is None:
            return 0, 0, ["Parsing not enabled"]
        
        parsed_count = 0
        total_items = 0
        all_errors: List[str] = []
        
        # Get supplier config
        supplier_config = self.supplier_config_map.get(supplier_name)
        if not supplier_config:
            logger.warning(f"No config found for supplier: {supplier_name}")
            return 0, 0, [f"No config for supplier: {supplier_name}"]
        
        # Download attachments
        try:
            paths = self.attachment_handler.download_attachments(attachments)
        except Exception as e:
            error_msg = f"Failed to download attachments: {str(e)}"
            logger.error(error_msg)
            return 0, 0, [error_msg]
        
        # Parse each attachment
        for attachment, file_path in zip(attachments, paths):
            try:
                logger.info(f"   └─ Parsing {attachment.filename}...")
                
                # Try to match brand from filename or parse all brand configs
                brand_configs = supplier_config.get('config', [])
                
                parsed_any = False
                for brand_config in brand_configs:
                    try:
                        # Parse the file
                        parsed_list = self.parser.parse_file(
                            file_path,
                            supplier_config,
                            brand_config
                        )
                        
                        # Store parsed data
                        gcs_path = self.storage.save_parsed_price_list(
                            parsed_list,
                            message_id,
                            email_date
                        )
                        
                        logger.info(
                            f"      ✅ Parsed {parsed_list.valid_rows} items for {brand_config['brand']}",
                            brand=brand_config['brand'],
                            items=parsed_list.valid_rows,
                            gcs_path=gcs_path
                        )
                        
                        parsed_count += 1
                        total_items += parsed_list.valid_rows
                        all_errors.extend(parsed_list.errors)
                        parsed_any = True
                        break  # Successfully parsed, move to next attachment
                        
                    except ParsingError as e:
                        # Try next brand config
                        continue
                
                if not parsed_any:
                    error_msg = f"Could not parse {attachment.filename} with any brand config"
                    logger.warning(f"      ⚠️  {error_msg}")
                    all_errors.append(error_msg)
                    
            except Exception as e:
                error_msg = f"Error parsing {attachment.filename}: {str(e)}"
                logger.error(f"      ❌ {error_msg}")
                all_errors.append(error_msg)
        
        return parsed_count, total_items, all_errors
    
    def process_email(self, message: Dict) -> EmailResult:
        """
        Process a single email message.
        
        Args:
            message: Gmail message object
            
        Returns:
            EmailResult with processing details
        """
        message_id = message['id']
        
        try:
            # Extract headers
            from_header = self.gmail_client.get_header(message, 'from') or ''
            subject = self.gmail_client.get_header(message, 'subject') or '(no subject)'
            date = self.gmail_client.get_message_date(message)
            
            # Log date extraction
            if date is None:
                logger.warning(
                    f"⚠️ Failed to extract date from message, using current time",
                    message_id=message_id,
                    has_internalDate='internalDate' in message
                )
            else:
                logger.info(
                    f"📅 Email date: {date.isoformat()}",
                    message_id=message_id,
                    email_date=date.isoformat()
                )
            
            # Detect supplier using three-layer detection
            from_address, from_domain, supplier_name, detection_method, original_sender = \
                self.detect_supplier_from_message(message, from_header)
            
            # Create result object
            result = EmailResult(
                message_id=message_id,
                from_address=from_address,
                from_domain=from_domain,
                subject=subject,
                date=date or datetime.now(timezone.utc),  # Use UTC-aware datetime.now()
                supplier_name=supplier_name,
                detection_method=detection_method,
                original_sender=original_sender
            )
            
            # Check if should ignore
            # Only ignore if NO supplier was detected AND domain is in ignore list
            if not supplier_name and self.should_ignore_email(from_address, from_domain):
                result.is_ignored = True
                logger.info(
                    f"📧 IGNORED | From: {from_address} | Subject: {subject[:60]}",
                    from_address=from_address,
                    from_domain=from_domain,
                    subject=subject,
                    message_id=message_id,
                    status="ignored"
                )
                return result
            
            # Log supplier detection result
            if supplier_name:
                log_msg = f"📧 SUPPLIER | {supplier_name} | From: {from_address}"
                if original_sender:
                    log_msg += f" | Original: {original_sender}"
                log_msg += f" | Method: {detection_method} | Subject: {subject[:60]}"
                logger.info(
                    log_msg,
                    supplier=supplier_name,
                    from_address=from_address,
                    from_domain=from_domain,
                    detection_method=detection_method,
                    original_sender=original_sender,
                    subject=subject,
                    message_id=message_id,
                    status="supplier"
                )
            else:
                result.is_unknown_domain = True
                log_msg = f"📧 UNKNOWN | From: {from_address} ({from_domain})"
                if original_sender:
                    log_msg += f" | Forwarded from: {original_sender}"
                log_msg += f" | Subject: {subject[:60]}"
                logger.info(
                    log_msg,
                    from_address=from_address,
                    from_domain=from_domain,
                    original_sender=original_sender,
                    subject=subject,
                    message_id=message_id,
                    status="unknown_domain"
                )
            
            # List attachments
            attachments = self.attachment_handler.list_attachments(message)
            result.attachments = attachments
            
            # Filter attachments by type
            supported, warning, ignored = self.attachment_handler.filter_attachments(attachments)
            result.supported_attachments = supported
            result.warning_attachments = warning
            result.ignored_attachments = ignored
            
            # Log attachment details if any
            if attachments:
                attachment_names = [a.filename for a in attachments]
                logger.info(
                    f"   └─ Attachments: {len(attachments)} total, {len(supported)} supported, {len(warning)} warnings",
                    message_id=message_id,
                    total_attachments=len(attachments),
                    supported_attachments=len(supported),
                    warning_attachments=len(warning),
                    attachment_files=attachment_names
                )
            
            # Parse attachments if this is from a known supplier and parsing is enabled
            if supplier_name and supported and self.enable_parsing:
                result.parsed_attachments, result.items_extracted, result.parsing_errors = \
                    self._parse_attachments(supported, supplier_name, message_id, date or datetime.now(timezone.utc))
            
            return result
            
        except Exception as e:
            logger.error(
                "Error processing email",
                error=str(e),
                message_id=message_id
            )
            result = EmailResult(
                message_id=message_id,
                from_address='',
                from_domain='',
                subject='',
                date=datetime.now(),
                error=str(e)
            )
            return result
    
    def process_emails(
        self,
        after_date: Optional[datetime] = None,
        max_emails: int = 100
    ) -> ProcessingResults:
        """
        Process emails from mailbox.
        
        Args:
            after_date: Only process emails after this date
            max_emails: Maximum number of emails to process
            
        Returns:
            ProcessingResults with summary and details
        """
        results = ProcessingResults()
        
        try:
            # Build query to filter emails
            # - Sent to pricing@ucalexports.com
            # - Exclude emails from ucalexports.com domain (internal emails)
            query = 'to:pricing@ucalexports.com -from:*@ucalexports.com'
            
            logger.info(f"Gmail query: {query}")
            
            messages = self.gmail_client.list_messages(
                query=query,
                after_date=after_date,
                max_results=max_emails
            )
            
            logger.info(
                f"{'='*80}\n📬 PROCESSING {len(messages)} EMAILS\n{'='*80}",
                count=len(messages),
                after_date=after_date.isoformat() if after_date else None
            )
            
            # Sort messages by internal date (oldest first) to process chronologically
            # Gmail returns newest first, but we want to process oldest first
            messages_with_dates = []
            for msg_metadata in messages:
                message = self.gmail_client.get_message(msg_metadata['id'])
                date = self.gmail_client.get_message_date(message)
                messages_with_dates.append((date, message))
            
            # Sort by date (oldest first)
            messages_with_dates.sort(key=lambda x: x[0] if x[0] else datetime.min)
            
            logger.info(
                f"Sorted {len(messages_with_dates)} emails chronologically (oldest first)"
            )
            
            # Process each message in chronological order
            for date, message in messages_with_dates:
                # Process email
                result = self.process_email(message)
                results.results.append(result)
                results.emails_processed += 1
                
                # Update counters
                if result.is_ignored:
                    results.emails_ignored += 1
                elif result.supplier_name:
                    results.emails_from_suppliers += 1
                elif result.is_unknown_domain:
                    results.emails_unknown_domain += 1
                
                # Count attachments
                results.total_attachments += len(result.attachments)
                results.supported_attachments += len(result.supported_attachments)
                results.warning_attachments += len(result.warning_attachments)
            
            logger.info(
                f"{'='*80}\n"
                f"✅ PROCESSING COMPLETE\n"
                f"   Total: {results.emails_processed} | "
                f"Suppliers: {results.emails_from_suppliers} | "
                f"Unknown: {results.emails_unknown_domain} | "
                f"Ignored: {results.emails_ignored} | "
                f"Attachments: {results.total_attachments}\n"
                f"{'='*80}",
                total=results.emails_processed,
                suppliers=results.emails_from_suppliers,
                unknown=results.emails_unknown_domain,
                ignored=results.emails_ignored,
                attachments=results.total_attachments
            )
            
        except Exception as e:
            error_msg = f"Failed to process emails: {e}"
            logger.error("Email processing failed", error=str(e))
            results.errors.append(error_msg)
        
        return results
    
    def cleanup(self):
        """Clean up resources."""
        self.attachment_handler.cleanup()
