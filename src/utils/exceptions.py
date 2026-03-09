"""
Custom exceptions for the email pricing bot.
"""


class EmailPricingBotError(Exception):
    """Base exception for all email pricing bot errors."""
    pass


class ConfigurationError(EmailPricingBotError):
    """Raised when there's an error in configuration."""
    pass


class AuthenticationError(EmailPricingBotError):
    """Raised when authentication fails."""
    pass


class EmailProcessingError(EmailPricingBotError):
    """Raised when email processing fails."""
    pass


class EmailError(EmailPricingBotError):
    """Raised when email operations fail."""
    pass


class FileParsingError(EmailPricingBotError):
    """Raised when file parsing fails."""
    pass


class DataValidationError(EmailPricingBotError):
    """Raised when data validation fails."""
    pass


class DriveUploadError(EmailPricingBotError):
    """Raised when Google Drive upload fails."""
    pass


class StateManagementError(EmailPricingBotError):
    """Raised when state management operations fail."""
    pass


class BrandDetectionError(EmailPricingBotError):
    """Raised when brand cannot be detected."""
    pass


class DateParsingError(EmailPricingBotError):
    """Raised when date parsing fails."""
    pass


class UnsupportedFileFormatError(EmailPricingBotError):
    """Raised when file format is not supported."""
    pass


class SupplierNotConfiguredError(EmailPricingBotError):
    """Raised when supplier is not in configuration."""
    pass


class ParsingError(EmailPricingBotError):
    """Raised when parsing price lists fails."""
    pass


class StorageError(EmailPricingBotError):
    """Raised when storage operations fail."""
    pass


class FileGenerationError(EmailPricingBotError):
    """Raised when file generation fails."""
    pass


class BigQueryProcessingError(EmailPricingBotError):
    """Raised when BigQuery processing operations fail."""
    pass


class ReconciliationError(EmailPricingBotError):
    """Raised when supersession reconciliation fails."""
    pass
