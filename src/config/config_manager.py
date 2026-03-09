"""
Configuration manager for loading and validating configuration files from GCS.
"""

import json
import os
from typing import Dict, List, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud.storage import Client, Bucket, Blob
from google.cloud import storage

from utils.logger import setup_logger
from utils.exceptions import ConfigurationError

logger = setup_logger(__name__)


class ConfigManager:
    """Manages loading and caching of configuration files from GCS."""
    
    # Environment variable for bucket name (can be overridden)
    BUCKET_ENV_VAR = 'GCS_BUCKET'
    
    def __init__(self, bucket_name: Optional[str] = None, use_test_config: bool = False):
        """
        Initialize configuration manager.
        
        Args:
            bucket_name: GCS bucket name (optional, can use env var)
            use_test_config: If True, load test configs (brand_config_test.json, etc.)
        """
        self.bucket_name = bucket_name or os.environ.get(self.BUCKET_ENV_VAR)
        self.use_test_config = use_test_config
        
        if not self.bucket_name:
            raise ConfigurationError(f"Bucket name must be provided or set in {self.BUCKET_ENV_VAR} environment variable")
        
        self.storage_client: 'Client' = storage.Client()
        self.bucket: 'Bucket' = self.storage_client.bucket(self.bucket_name)
        
        config_mode = "TEST" if use_test_config else "PRODUCTION"
        logger.info(f"ConfigManager initialized ({config_mode})", bucket=self.bucket_name)
    
    def load_core_config(self) -> Dict[str, Any]:
        """
        Load core configuration from GCS.
        
        Returns:
            Core configuration dictionary
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config = self._load_json_from_gcs('config/core/core_config.json')
            if not isinstance(config, dict):
                raise ConfigurationError("Core config must be a dictionary")
            self._validate_core_config(config)
            logger.info("Core configuration loaded successfully")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load core config: {str(e)}")
            raise ConfigurationError(f"Failed to load core config: {str(e)}")
    
    def load_supplier_config(self) -> List[Dict[str, Any]]:
        """
        Load supplier configuration from GCS.
        
        Returns:
            List of supplier configuration dictionaries
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config = self._load_json_from_gcs('config/supplier/supplier_config.json')
            if not isinstance(config, list):
                raise ConfigurationError("Supplier config must be a list")
            self._validate_supplier_config(config)
            logger.info(
                "Supplier configuration loaded successfully",
                supplier_count=len(config)
            )
            return config
            
        except Exception as e:
            logger.error(f"Failed to load supplier config: {str(e)}")
            raise ConfigurationError(f"Failed to load supplier config: {str(e)}")
    
    def load_brand_config(self) -> List[Dict[str, Any]]:
        """
        Load brand configuration from GCS.
        
        Uses config/brand/brand_config_test.json if use_test_config=True, otherwise config/brand/brand_config.json.
        
        Returns:
            List of brand configuration dictionaries
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config_file = 'config/brand/brand_config_test.json' if self.use_test_config else 'config/brand/brand_config.json'
            logger.debug(f"Loading brand config from: {config_file}")
            config = self._load_json_from_gcs(config_file)
            if not isinstance(config, list):
                raise ConfigurationError("Brand config must be a list")
            
            self._validate_brand_config(config)
            
            # Log sample of brands with drive folder IDs for verification
            brands_with_folders = [b['brand'] for b in config if b.get('driveFolderId')]
            logger.info(
                "Brand configuration loaded successfully",
                brand_count=len(config),
                brands_with_drive_folders=len(brands_with_folders),
                sample_brands=brands_with_folders[:5]
            )
            return config
            
        except Exception as e:
            logger.error(f"Failed to load brand config: {str(e)}")
            raise ConfigurationError(f"Failed to load brand config: {str(e)}")
    
    def load_scraper_config(self) -> List[Dict[str, Any]]:
        """
        Load scraper configuration from GCS.
        
        Returns:
            List of scraper configuration dictionaries
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config = self._load_json_from_gcs('config/scraper/scraper_config.json')
            if not isinstance(config, list):
                raise ConfigurationError("Scraper config must be a list")
            self._validate_scraper_config(config)
            logger.info(
                "Scraper configuration loaded successfully",
                scraper_count=len(config)
            )
            return config
            
        except Exception as e:
            logger.error(f"Failed to load scraper config: {str(e)}")
            raise ConfigurationError(f"Failed to load scraper config: {str(e)}")
    
    def load_column_mapping_config(self) -> Dict[str, Any]:
        """
        Load column mapping configuration from GCS.
        
        Returns:
            Column mapping configuration dictionary
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config = self._load_json_from_gcs('config/core/column_mapping_config.json')
            if not isinstance(config, dict):
                raise ConfigurationError("Column mapping config must be a dictionary")
            logger.info("Column mapping configuration loaded successfully")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load column mapping config: {str(e)}")
            raise ConfigurationError(f"Failed to load column mapping config: {str(e)}")
    
    def load_currency_config(self) -> Dict[str, Any]:
        """
        Load currency configuration from GCS.
        
        Returns:
            Currency configuration dictionary
            
        Raises:
            ConfigurationError: If config cannot be loaded or is invalid
        """
        try:
            config = self._load_json_from_gcs('config/core/currency_config.json')
            if not isinstance(config, dict):
                raise ConfigurationError("Currency config must be a dictionary")
            logger.info("Currency configuration loaded successfully")
            return config
            
        except Exception as e:
            logger.error(f"Failed to load currency config: {str(e)}")
            raise ConfigurationError(f"Failed to load currency config: {str(e)}")
    
    def load_all_configs(self) -> Dict[str, Any]:
        """
        Load all configuration files.
        
        Returns:
            Dictionary containing all configs:
            {
                'core': core_config,
                'suppliers': supplier_config,
                'brands': brand_config,
                'scrapers': scraper_config,
                'column_mapping': column_mapping_config,
                'currency': currency_config
            }
        """
        return {
            'core': self.load_core_config(),
            'suppliers': self.load_supplier_config(),
            'brands': self.load_brand_config(),
            'scrapers': self.load_scraper_config(),
            'column_mapping': self.load_column_mapping_config(),
            'currency': self.load_currency_config()
        }
    
    def _load_json_from_gcs(self, blob_path: str) -> Dict[str, Any] | List[Dict[str, Any]]:
        """
        Load JSON file from GCS.
        
        Args:
            blob_path: Path to blob within bucket
            
        Returns:
            Parsed JSON data
            
        Raises:
            ConfigurationError: If file cannot be loaded or parsed
        """
        try:
            blob: 'Blob' = self.bucket.blob(blob_path)
            
            if not blob.exists():
                raise ConfigurationError(f"Configuration file not found: {blob_path}")
            
            content: str = blob.download_as_text()
            
            data: Dict[str, Any] | List[Dict[str, Any]] = json.loads(content)
            
            logger.debug(f"Loaded JSON from GCS", path=blob_path)
            return data
            
        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in {blob_path}: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load {blob_path}: {str(e)}")
    
    def _validate_core_config(self, config: Dict[str, Any]) -> None:
        """
        Validate core configuration structure.
        
        Args:
            config: Core configuration dictionary
            
        Raises:
            ConfigurationError: If validation fails
        """
        required_keys = ['gmail', 'gcp', 'execution', 'defaults', 'notification']
        
        for key in required_keys:
            if key not in config:
                raise ConfigurationError(f"Missing required key in core config: {key}")
        
        # Validate gmail section
        gmail_keys = ['delegated_user_email', 'own_domain']
        for key in gmail_keys:
            if key not in config['gmail']:
                raise ConfigurationError(f"Missing gmail.{key} in core config")
        
        # Validate ignore_emails if present
        if 'ignore_emails' in config['gmail']:
            if not isinstance(config['gmail']['ignore_emails'], list):
                raise ConfigurationError("gmail.ignore_emails must be a list")
        
        # Validate gcp section
        gcp_keys = ['project_id', 'secret_name', 'bucket_name', 'state_file']
        for key in gcp_keys:
            if key not in config['gcp']:
                raise ConfigurationError(f"Missing gcp.{key} in core config")
        
        # Validate execution section
        exec_keys = ['schedule_hour', 'schedule_minute', 'timezone']
        for key in exec_keys:
            if key not in config['execution']:
                raise ConfigurationError(f"Missing execution.{key} in core config")
        
        # Validate schedule values
        hour = config['execution']['schedule_hour']
        minute = config['execution']['schedule_minute']
        
        if not (0 <= hour <= 23):
            raise ConfigurationError(f"Invalid schedule_hour: {hour} (must be 0-23)")
        
        if not (0 <= minute <= 59):
            raise ConfigurationError(f"Invalid schedule_minute: {minute} (must be 0-59)")
        
        # Validate notification section
        if 'summary_email_recipients' not in config['notification']:
            raise ConfigurationError("Missing notification.summary_email_recipients")
        
        if not config['notification']['summary_email_recipients']:
            raise ConfigurationError("summary_email_recipients cannot be empty")
        
        # Validate summary_mode
        summary_mode = config['notification'].get('summary_mode', 'immediate')
        if summary_mode not in ['immediate', 'daily']:
            raise ConfigurationError(f"Invalid notification.summary_mode: {summary_mode} (must be 'immediate' or 'daily')")
        
        # Validate daily summary time if in daily mode
        if summary_mode == 'daily':
            daily_hour = config['notification'].get('daily_summary_hour')
            daily_minute = config['notification'].get('daily_summary_minute')
            
            if daily_hour is None:
                raise ConfigurationError("Missing notification.daily_summary_hour (required when summary_mode is 'daily')")
            if daily_minute is None:
                raise ConfigurationError("Missing notification.daily_summary_minute (required when summary_mode is 'daily')")
            
            if not (0 <= daily_hour <= 23):
                raise ConfigurationError(f"Invalid daily_summary_hour: {daily_hour} (must be 0-23)")
            
            if not (0 <= daily_minute <= 59):
                raise ConfigurationError(f"Invalid daily_summary_minute: {daily_minute} (must be 0-59)")
        
        # Validate bigquery section (required for supersession reconciliation)
        if 'bigquery' not in config:
            raise ConfigurationError("Missing required 'bigquery' section in core config")
        
        bq_config = config['bigquery']
        
        # Required fields
        bq_required = ['project_id', 'dataset_id']
        for key in bq_required:
            if key not in bq_config:
                raise ConfigurationError(f"Missing bigquery.{key} in core config")
        
        # Validate reconciliation config if present
        if 'reconciliation' in bq_config:
            recon = bq_config['reconciliation']
            max_depth = recon.get('max_chain_depth', 10)
            if not isinstance(max_depth, int) or max_depth < 1 or max_depth > 100:
                raise ConfigurationError(
                    f"Invalid bigquery.reconciliation.max_chain_depth: {max_depth} (must be 1-100)"
                )
        
        # Validate cleanup_on_failure mode if present
        if 'cleanup_on_failure' in bq_config:
            cleanup = bq_config['cleanup_on_failure']
            mode = cleanup.get('mode', 'test')
            if mode not in ['test', 'production']:
                raise ConfigurationError(
                    f"Invalid bigquery.cleanup_on_failure.mode: {mode} (must be 'test' or 'production')"
                )
    
    def _validate_supplier_config(self, config: List[Dict[str, Any]]) -> None:
        """
        Validate supplier configuration structure.
        
        Args:
            config: List of supplier configuration dictionaries
            
        Raises:
            ConfigurationError: If validation fails
        """
        # config is already validated to be a list in the calling method
        
        if not config:
            raise ConfigurationError("Supplier config cannot be empty")
        
        supplier_names: set[str] = set()
        email_domains: set[str] = set()
        
        for idx, supplier in enumerate(config):
            # Check required fields
            if 'supplier' not in supplier:
                raise ConfigurationError(f"Supplier at index {idx} missing 'supplier' field")
            
            # Either email_domain OR email_address OR email_addresses must be present
            has_email_domain = 'email_domain' in supplier and supplier['email_domain']
            has_email_address = 'email_address' in supplier and supplier['email_address']
            has_email_addresses = 'email_addresses' in supplier and supplier['email_addresses']
            
            if not has_email_domain and not has_email_address and not has_email_addresses:
                raise ConfigurationError(
                    f"Supplier '{supplier['supplier']}' must have either 'email_domain', 'email_address', or 'email_addresses'"
                )
            
            # Validate email_addresses is a list if present
            if has_email_addresses:
                if not isinstance(supplier['email_addresses'], list):
                    raise ConfigurationError(
                        f"Supplier '{supplier['supplier']}' email_addresses must be a list"
                    )
                if not supplier['email_addresses']:
                    raise ConfigurationError(
                        f"Supplier '{supplier['supplier']}' email_addresses cannot be empty"
                    )
            
            if 'config' not in supplier:
                raise ConfigurationError(
                    f"Supplier '{supplier['supplier']}' missing 'config' field"
                )
            
            # Check for duplicates
            supplier_name = supplier['supplier']
            if supplier_name in supplier_names:
                raise ConfigurationError(f"Duplicate supplier name: {supplier_name}")
            supplier_names.add(supplier_name)
            
            # Track email domains (if present) for duplicate detection
            if has_email_domain:
                email_domain = supplier['email_domain']
                # Support both single string and array of domains
                domains_to_check = email_domain if isinstance(email_domain, list) else [email_domain]
                for domain in domains_to_check:
                    if domain in email_domains:
                        logger.debug(
                            f"Duplicate email domain",
                            domain=domain,
                            supplier=supplier_name
                        )
                    email_domains.add(domain)
            
            # Validate discount if present
            if 'discount_percent' in supplier:
                discount = supplier['discount_percent']
                if not (0 <= discount <= 100):
                    raise ConfigurationError(
                        f"Invalid discount_percent for {supplier_name}: {discount}"
                    )
            
            # Validate config array
            if not isinstance(supplier['config'], list) or not supplier['config']:
                raise ConfigurationError(
                    f"Supplier '{supplier_name}' config must be non-empty list"
                )
            
            # Validate each brand config
            for brand_idx, brand_config in enumerate(supplier['config']):
                if isinstance(brand_config, dict):
                    self._validate_brand_config_entry(
                        brand_config, supplier_name, brand_idx, supplier
                    )
    
    def _validate_brand_config_entry(
        self, brand_config: Dict[str, Any], supplier_name: str, idx: int, supplier_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Validate a single brand configuration entry within supplier config."""
        # All parsing uses intelligent header detection (no column config needed)
        
        # Brand field is always required
        if 'brand' not in brand_config:
            raise ConfigurationError(
                f"Supplier '{supplier_name}' brand config {idx} missing 'brand' field"
            )
        
        # Get supplier-level defaults (flattened structure - top-level fields)
        # Also check legacy metadata for backwards compatibility
        supplier_defaults: Dict[str, Any] = {}
        if supplier_config:
            supplier_metadata = supplier_config.get('metadata', {})
            supplier_defaults = {
                'location': supplier_config.get('location') or supplier_metadata.get('location'),
                'currency': supplier_config.get('currency') or supplier_metadata.get('currency'),
                'decimalFormat': supplier_config.get('decimalFormat') or supplier_metadata.get('decimalFormat')
            }
        
        # location, currency, and decimalFormat can be at brand level OR supplier level
        # They're required in at least one place
        fields_to_validate = ['location', 'currency', 'decimalFormat']
        
        for field in fields_to_validate:
            brand_has_field = field in brand_config
            supplier_has_field = supplier_defaults.get(field) is not None
            
            if not brand_has_field and not supplier_has_field:
                raise ConfigurationError(
                    f"Supplier '{supplier_name}' brand config {idx}: '{field}' must be specified "
                    f"either at brand level or at supplier level"
                )
        
        # Validate decimal format if present at brand level (case-insensitive)
        # Note: Only "comma" and "decimal" are valid. Migration script converts "european" to "comma"
        if 'decimalFormat' in brand_config:
            decimal_format = brand_config['decimalFormat'].lower()
            if decimal_format not in ['decimal', 'comma']:
                raise ConfigurationError(
                    f"Invalid decimalFormat for {supplier_name} brand config {idx}: "
                    f"{brand_config['decimalFormat']} (expected 'Comma'/'comma' or 'Decimal'/'decimal')"
                )
        
        # Validate supplier-level decimal format if present
        if supplier_defaults.get('decimalFormat'):
            supplier_decimal_format = supplier_defaults['decimalFormat'].lower()
            if supplier_decimal_format not in ['decimal', 'comma']:
                raise ConfigurationError(
                    f"Invalid decimalFormat for {supplier_name}: "
                    f"{supplier_defaults['decimalFormat']} (expected 'Comma'/'comma' or 'Decimal'/'decimal')"
                )
        
        # All parsing uses intelligent header detection (no column config needed)
    
    def _validate_brand_config(self, config: List[Dict[str, Any]]) -> None:
        """
        Validate brand configuration structure.
        
        Args:
            config: List of brand configuration dictionaries
            
        Raises:
            ConfigurationError: If validation fails
        """
        # config is already validated to be a list in the calling method
        
        if not config:
            raise ConfigurationError("Brand config cannot be empty")
        
        brand_names: set[str] = set()
        all_aliases: set[str] = set()
        
        for idx, brand in enumerate(config):
            # Check required fields
            required_fields = ['brand', 'minimumPartLength', 'driveFolderId']
            for field in required_fields:
                if field not in brand:
                    raise ConfigurationError(
                        f"Brand at index {idx} missing '{field}' field"
                    )
            
            # Check for duplicate brand names
            brand_name = brand['brand']
            if brand_name in brand_names:
                raise ConfigurationError(f"Duplicate brand name: {brand_name}")
            brand_names.add(brand_name)
            
            # Validate minimum part length
            min_length = brand['minimumPartLength']
            if not isinstance(min_length, int) or min_length <= 0:
                raise ConfigurationError(
                    f"Invalid minimumPartLength for {brand_name}: {min_length}"
                )
            
            # Check aliases for duplicates (case-insensitive)
            if 'aliases' in brand:
                for alias in brand['aliases']:
                    alias_lower = alias.lower()
                    if alias_lower in all_aliases:
                        raise ConfigurationError(
                            f"Duplicate alias '{alias}' found for brand {brand_name}"
                        )
                    all_aliases.add(alias_lower)
    
    def _validate_scraper_config(self, config: List[Dict[str, Any]]) -> None:
        """
        Validate scraper configuration structure.
        
        Args:
            config: List of scraper configuration dictionaries
            
        Raises:
            ConfigurationError: If validation fails
        """
        # config is already validated to be a list in the calling method
        
        if not config:
            logger.warning("Scraper config is empty")
            return
        
        supplier_names: set[str] = set()
        
        for idx, scraper in enumerate(config):
            # Check required fields
            required_fields = ['supplier', 'type', 'enabled', 'schedule', 'metadata']
            for field in required_fields:
                if field not in scraper:
                    raise ConfigurationError(
                        f"Scraper at index {idx} missing '{field}' field"
                    )
            
            # Check for duplicate supplier names
            supplier_name = scraper['supplier']
            if supplier_name in supplier_names:
                raise ConfigurationError(f"Duplicate supplier name: {supplier_name}")
            supplier_names.add(supplier_name)
            
            # Validate scraper type
            scraper_type = scraper['type']
            valid_types = ['custom', 'api_client', 'link_downloader', 'email_trigger', 'directory_listing', 'webdav', 'form_export']
            if scraper_type not in valid_types:
                raise ConfigurationError(
                    f"Invalid scraper type for {supplier_name}: {scraper_type}. "
                    f"Valid types: {valid_types}"
                )
            
            # Validate schedule
            schedule = scraper['schedule']
            required_schedule_fields = ['frequency', 'time', 'timezone']
            for field in required_schedule_fields:
                if field not in schedule:
                    raise ConfigurationError(
                        f"Scraper '{supplier_name}' schedule missing '{field}'"
                    )
            
            # Validate frequency
            frequency = schedule['frequency']
            if frequency not in ['daily', 'weekly', 'monthly']:
                raise ConfigurationError(
                    f"Invalid frequency for {supplier_name}: {frequency}"
                )
            
            # Validate authentication if present
            if 'authentication' in scraper:
                auth = scraper['authentication']
                if 'method' not in auth:
                    raise ConfigurationError(
                        f"Scraper '{supplier_name}' authentication missing 'method'"
                    )
                
                auth_method = auth['method']
                if auth_method not in ['form', 'bearer', 'basic', 'none']:
                    raise ConfigurationError(
                        f"Invalid auth method for {supplier_name}: {auth_method}"
                    )
                
                # Validate method-specific fields
                if auth_method == 'form':
                    required_form_fields = ['login_url', 'username_field', 'password_field', 'submit_button']
                    for field in required_form_fields:
                        if field not in auth:
                            raise ConfigurationError(
                                f"Scraper '{supplier_name}' form auth missing '{field}'"
                            )
                
                if auth_method in ['form', 'basic']:
                    if 'username_env' not in auth or 'password_env' not in auth:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' {auth_method} auth missing username_env/password_env"
                        )
            
            # Validate type-specific configuration
            if scraper_type == 'api_client':
                api = scraper.get('api', {})
                required_api_fields = ['base_url', 'list_endpoint', 'list_method', 'list_items_path', 'export_endpoint', 'export_method', 'export_params_template']
                for field in required_api_fields:
                    if field not in api:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' api missing '{field}'"
                        )
            
            elif scraper_type == 'link_downloader':
                links = scraper.get('links', {})
                required_links_fields = ['page_url', 'link_selector', 'link_href_pattern']
                for field in required_links_fields:
                    if field not in links:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' links missing '{field}'"
                        )
            
            elif scraper_type == 'email_trigger':
                email_trigger = scraper.get('email_trigger', {})
                required_fields = ['page_url', 'trigger_selector', 'success_selector']
                for field in required_fields:
                    if field not in email_trigger:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' email_trigger missing '{field}'"
                        )
            
            elif scraper_type == 'directory_listing':
                directory = scraper.get('directory', {})
                required_fields = ['base_url', 'include_glob']
                for field in required_fields:
                    if field not in directory:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' directory missing '{field}'"
                        )
            
            elif scraper_type == 'webdav':
                webdav = scraper.get('webdav', {})
                required_fields = ['base_url', 'path', 'include_glob']
                for field in required_fields:
                    if field not in webdav:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' webdav missing '{field}'"
                        )
            
            elif scraper_type == 'form_export':
                form = scraper.get('form', {})
                required_fields = ['page_url', 'brand_select_selector', 'export_button_selector', 'result_link_selector']
                for field in required_fields:
                    if field not in form:
                        raise ConfigurationError(
                            f"Scraper '{supplier_name}' form missing '{field}'"
                        )
