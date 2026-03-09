"""
Configuration schemas and validation helpers.

Provides type definitions and validation functions for configuration files.
"""

from typing import TypedDict, List, Optional, Union


class GmailConfig(TypedDict):
    """Gmail configuration schema."""
    delegated_user_email: str
    own_domain: str
    ignore_emails: Optional[List[str]]


class GCPConfig(TypedDict):
    """GCP resources configuration schema."""
    project_id: str
    secret_name: str
    bucket_name: str
    state_file: str


class ExecutionConfig(TypedDict):
    """Execution schedule configuration schema."""
    schedule_hour: int
    schedule_minute: int
    timezone: str


class DefaultsConfig(TypedDict):
    """System defaults configuration schema."""
    expiry_duration_days: int


class NotificationConfig(TypedDict):
    """Notification configuration schema."""
    summary_email_recipients: List[str]
    send_from_group: bool
    summary_mode: str  # "immediate" or "daily"
    daily_summary_hour: int  # Hour to send daily summary (0-23)
    daily_summary_minute: int  # Minute to send daily summary (0-59)
    summary_from_email: Optional[str]  # Optional sender email address


class CoreConfig(TypedDict):
    """Core configuration schema."""
    version: str
    gmail: GmailConfig
    gcp: GCPConfig
    execution: ExecutionConfig
    defaults: DefaultsConfig
    notification: NotificationConfig


class ColumnMapping(TypedDict):
    """Column mapping schema for supplier files."""
    partNumber: Union[int, str]  # int or "null"
    description: Union[int, str]
    formerPartNumber: Union[int, str]
    supersedePartNumber: Union[int, str]
    price: int


class BrandConfigEntry(TypedDict, total=False):
    """Brand configuration entry within supplier config."""
    brand: str
    enabled: Optional[bool]  # Optional per-brand enable/disable flag (default: True)
    location: Optional[str]  # Can be at brand level or inherited from supplier level
    currency: Optional[str]  # Can be at brand level or inherited from supplier level
    partNumberSplice: Optional[int]
    decimalFormat: Optional[str]  # Can be at brand level or inherited from supplier level
    columnheader: Optional[str]
    gst_column: Optional[str]
    discount: Optional[float]  # Per-brand discount override
    packagingpercent: Optional[float]  # Packaging cost percentage to add to prices
    # columns: ColumnMapping  # Deprecated - no longer used (intelligent header detection)


class SupplierConfig(TypedDict, total=False):
    """Email supplier configuration schema (flattened structure)."""
    supplier: str
    email_domain: Union[str, List[str]]
    email_addresses: Optional[List[str]]  # Alternative to email_domain for specific addresses
    discount_percent: Optional[float]
    default_brand: Optional[str]
    default_expiry_days: Optional[int]
    # Flattened top-level fields (moved from metadata)
    location: Optional[str]  # Supplier-level default location
    currency: Optional[str]  # Supplier-level default currency
    decimalFormat: Optional[str]  # Supplier-level default decimal format
    ignore_brands: Optional[List[str]]  # Brands to skip for this supplier
    brand_aliases: Optional[dict]  # Map of short codes to canonical brand names
    config: List[BrandConfigEntry]


class BrandConfig(TypedDict):
    """Brand configuration schema."""
    brand: str
    aliases: Optional[List[str]]
    minimumPartLength: int
    driveFolderId: str


class StateFile(TypedDict):
    """State file schema."""
    last_processed_timestamp: str
    last_execution_timestamp: str
    version: str


# Output file format constants
OUTPUT_FILE_NAME_FORMAT = "{brand}_{supplier}_{location}_{currency}_{date}.csv"

# Output CSV columns in order
OUTPUT_COLUMNS = [
    'Brand',
    'Supplier Name',
    'Location',
    'Currency',
    'Part Number',
    'Description',
    'Former PN',
    'Supersession',
    'Price'
]

# Supported file extensions
SUPPORTED_EXTENSIONS = ['.csv', '.xlsx']
WARNING_EXTENSIONS = ['.xls', '.pdf']

# Date format for output filename (MMMYY format like SEP18)
OUTPUT_DATE_FORMAT = "%b%d"  # e.g., "SEP18"


# Scraper configuration schemas
class ScraperScheduleConfig(TypedDict):
    """Scraper schedule configuration schema."""
    frequency: str  # 'daily', 'weekly', 'monthly'
    day_of_week: Optional[str]  # For weekly: 'monday', 'tuesday', etc.
    day_of_month: Optional[int]  # For monthly: 1-31
    time: str  # 'HH:MM' format
    timezone: str


class ScraperAuthConfig(TypedDict):
    """Scraper authentication configuration schema."""
    method: str  # 'form', 'bearer', 'basic', 'none'
    login_url: Optional[str]
    username_field: Optional[str]
    password_field: Optional[str]
    submit_button: Optional[str]
    username_env: Optional[str]  # Environment variable for username
    password_env: Optional[str]  # Environment variable for password
    token_header: Optional[str]  # Header name for bearer token
    token_value: Optional[str]  # Token value (can use env var substitution)


class ScraperNavigationStep(TypedDict):
    """Single navigation step configuration."""
    action: str  # 'click', 'wait', 'fill'
    selector: str
    wait_for: Optional[str]  # 'load', 'networkidle', 'domcontentloaded'
    value: Optional[str]  # For fill actions


class ScraperDownloadConfig(TypedDict):
    """Scraper download configuration schema."""
    method: str  # 'click', 'api', 'direct'
    selector: Optional[str]
    expected_file_pattern: Optional[str]


class ScraperMetadataConfig(TypedDict):
    """Scraper metadata configuration schema (deprecated - fields moved to top-level)."""
    brand: Optional[str]
    detect_expiry_from: Optional[str]  # 'page_text', 'filename', 'config'
    expiry_selector: Optional[str]
    # Note: location, currency, decimalFormat, default_expiry_days moved to top-level in ScraperConfig


# Type-specific configuration schemas
class ScraperApiConfig(TypedDict):
    """API client scraper configuration schema."""
    base_url: str
    list_endpoint: str
    list_method: str
    list_params: Optional[dict]
    list_items_path: str
    export_endpoint: str
    export_method: str
    export_params_template: dict
    headers: Optional[dict]


class ScraperLinksConfig(TypedDict):
    """Link downloader scraper configuration schema."""
    page_url: str
    link_selector: str
    link_href_pattern: str
    filename_from: Optional[str]  # 'href' or 'text'


class ScraperEmailTriggerConfig(TypedDict):
    """Email trigger scraper configuration schema."""
    page_url: str
    trigger_selector: str
    success_selector: str
    per_brand: Optional[bool]
    brand_param_key: Optional[str]


class ScraperDirectoryConfig(TypedDict):
    """Directory listing scraper configuration schema."""
    base_url: str
    include_glob: str
    exclude_glob: Optional[str]


class ScraperWebdavConfig(TypedDict):
    """WebDAV scraper configuration schema."""
    base_url: str
    path: str
    include_glob: str


class ScraperFormConfig(TypedDict):
    """Form export scraper configuration schema."""
    page_url: str
    brand_select_selector: str
    export_button_selector: str
    result_link_selector: str


class ScraperConfig(TypedDict):
    """
    Scraper configuration schema (flattened structure).
    
    The 'config' array is the single source of truth for enabled brands.
    Brands with 'enabled: false' are skipped during scraping.
    """
    supplier: str
    type: str  # 'api_client', 'link_downloader', 'email_trigger', 'directory_listing', 'webdav', 'form_export'
    enabled: bool
    schedule: ScraperScheduleConfig
    authentication: Optional[ScraperAuthConfig]
    api: Optional[ScraperApiConfig]  # For api_client type
    links: Optional[ScraperLinksConfig]  # For link_downloader type
    email_trigger: Optional[ScraperEmailTriggerConfig]  # For email_trigger type
    directory: Optional[ScraperDirectoryConfig]  # For directory_listing type
    webdav: Optional[ScraperWebdavConfig]  # For webdav type
    form: Optional[ScraperFormConfig]  # For form_export type
    # Flattened top-level fields (moved from metadata)
    location: Optional[str]  # Supplier-level default location
    currency: Optional[str]  # Supplier-level default currency
    decimalFormat: Optional[str]  # Supplier-level default decimal format
    default_expiry_days: Optional[int]  # Supplier-level default expiry days
    metadata: Optional[ScraperMetadataConfig]  # Remaining metadata fields (optional)
    custom_scraper_class: Optional[str]  # For complex sites requiring custom code
    config: List[BrandConfigEntry]  # Brand-specific configurations (single source of enabled brands)
