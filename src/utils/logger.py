"""
Logging utilities for the email pricing bot.

Provides structured logging with context for Cloud Functions.
Uses google-cloud-logging in Cloud Functions, JSON stdout locally.
"""

import logging
import sys
import json
import os
from datetime import datetime
from typing import Any, Dict


class StructuredLogger:
    """Structured logger for Cloud Functions with JSON output."""
    
    def __init__(self, name: str, cloud_logging_active: bool = False):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # If Cloud Logging is active, don't add handlers (Cloud Logging handles it)
        # Otherwise, add console handler for local/development use
        if not cloud_logging_active:
            # Remove existing handlers
            self.logger.handlers = []
            
            # Create console handler with JSON formatter
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.INFO)
            handler.setFormatter(JsonFormatter())
            
            self.logger.addHandler(handler)
            
            # Prevent propagation to avoid duplicate logs when using our own handler
            self.logger.propagate = False
        else:
            # When Cloud Logging is active, ALLOW propagation so logs reach root logger
            # (Cloud Logging adds handlers to root logger via client.setup_logging())
            self.logger.propagate = True
    
    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """Internal log method with structured data."""
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'severity': level.upper(),
            'message': message,
            **kwargs
        }
        
        getattr(self.logger, level.lower())(json.dumps(log_data))
    
    def info(self, message: str, **kwargs: Any) -> None:
        """Log info level message."""
        self._log('INFO', message, **kwargs)
    
    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning level message."""
        self._log('WARNING', message, **kwargs)
    
    def error(self, message: str, **kwargs: Any) -> None:
        """Log error level message."""
        self._log('ERROR', message, **kwargs)
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug level message."""
        self._log('DEBUG', message, **kwargs)


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        try:
            # If message is already JSON, parse and use it
            log_data = json.loads(record.getMessage())
        except (json.JSONDecodeError, ValueError):
            # Otherwise create structured log
            log_data = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'severity': record.levelname,
                'message': record.getMessage(),
                'logger': record.name,
            }
            
            if record.exc_info:
                log_data['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


def _is_cloud_environment() -> bool:
    """Check if running in Google Cloud Functions/Cloud Run."""
    return bool(os.getenv('FUNCTION_NAME') or os.getenv('K_SERVICE'))


def _setup_cloud_logging() -> bool:
    """
    Initialize Google Cloud Logging.
    
    Returns:
        True if successfully initialized, False otherwise
    """
    try:
        import google.cloud.logging
        client = google.cloud.logging.Client()
        client.setup_logging()
        return True
    except Exception as e:
        # Fallback to local logging if Cloud Logging fails
        print(f"Warning: Failed to initialize Cloud Logging: {e}", file=sys.stderr)
        return False


# Initialize Cloud Logging once at module load if in cloud environment
_cloud_logging_initialized = False
if _is_cloud_environment():
    _cloud_logging_initialized = _setup_cloud_logging()


def setup_logger(name: str) -> StructuredLogger:
    """
    Set up a structured logger for the given name.
    
    In Cloud Functions: Uses StructuredLogger wrapper with Cloud Logging backend
    Locally: Uses StructuredLogger with JSON output
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        StructuredLogger instance
    """
    # Pass cloud_logging_active flag to avoid duplicate handlers
    return StructuredLogger(name, cloud_logging_active=_cloud_logging_initialized)


def get_logger(name: str) -> StructuredLogger:
    """
    Get a structured logger for the given name.
    
    Alias for setup_logger for consistency with common logging patterns.
    """
    return setup_logger(name)
