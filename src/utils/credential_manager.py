"""
Credential Manager for Web Scrapers

Centralized utility for retrieving and validating scraper credentials.
Ensures credentials are available before scraping begins, with clear error reporting.
"""

import os
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ScraperCredentials:
    """Container for scraper credentials."""
    username: str
    password: str


class CredentialError(Exception):
    """Raised when credentials are missing or invalid."""
    pass


class CredentialManager:
    """
    Manages retrieval and validation of scraper credentials.
    
    Credentials are loaded from:
    1. Environment variables (highest priority): SCRAPER_{SUPPLIER}_USERNAME, SCRAPER_{SUPPLIER}_PASSWORD
    2. Scraper config (fallback)
    3. Google Secret Manager (via environment variables in Cloud Run)
    """
    
    def __init__(self, supplier_name: str, auth_config: Optional[Dict[str, Any]] = None):
        """
        Initialize credential manager for a supplier.
        
        Args:
            supplier_name: Name of the supplier (e.g., "MATEROM", "AUTOCAR")
            auth_config: Optional authentication config dict from scraper_config.json
        """
        self.supplier_name = supplier_name.upper()
        self.auth_config = auth_config or {}
    
    def get_credentials(self) -> ScraperCredentials:
        """
        Retrieve and validate credentials for the supplier.
        
        Returns:
            ScraperCredentials: Container with username and password
            
        Raises:
            CredentialError: If username or password is missing
        """
        username = self._get_credential("username")
        password = self._get_credential("password")
        
        # Validate both credentials are present
        missing: list[str] = []
        if not username:
            missing.append("username")
        if not password:
            missing.append("password")
        
        if missing:
            error_msg = (
                f"Missing required credentials for {self.supplier_name}: "
                f"{', '.join(missing)}. "
                f"Please set environment variables or Secret Manager secrets: "
            )
            
            # Build helpful error message with specific secret names
            secret_names: list[str] = []
            if "username" in missing:
                secret_names.append(f"SCRAPER_{self.supplier_name}_USERNAME")
            if "password" in missing:
                secret_names.append(f"SCRAPER_{self.supplier_name}_PASSWORD")
            
            error_msg += ", ".join(secret_names)
            
            logger.error(
                f"Credential validation failed for {self.supplier_name}",
                extra={
                    "supplier": self.supplier_name,
                    "missing_credentials": missing,
                    "required_env_vars": secret_names,
                    "has_auth_config": bool(self.auth_config)
                }
            )
            raise CredentialError(error_msg)
        
        # Type assertion: at this point we know both are not None due to validation above
        assert username is not None, "Username should not be None after validation"
        assert password is not None, "Password should not be None after validation"
        
        # Mask password in logs
        masked_password = password[:2] + "***" if len(password) > 2 else "***"
        logger.info(
            f"Successfully retrieved credentials for {self.supplier_name}",
            extra={
                "supplier": self.supplier_name,
                "username": username,
                "password": masked_password
            }
        )
        
        return ScraperCredentials(username=username, password=password)
    
    def _get_credential(self, credential_type: str) -> Optional[str]:
        """
        Get a single credential from environment or config.
        
        Args:
            credential_type: Type of credential ("username" or "password")
            
        Returns:
            Credential value or None if not found
        """
        # Try environment variable first (includes Secret Manager in Cloud Run)
        env_key = f"SCRAPER_{self.supplier_name}_{credential_type.upper()}"
        env_value = os.getenv(env_key)
        
        if env_value:
            logger.debug(
                f"Loaded {credential_type} from environment variable",
                extra={
                    "supplier": self.supplier_name,
                    "env_key": env_key,
                    "source": "environment"
                }
            )
            return env_value
        
        # Fallback to auth_config
        config_value = self.auth_config.get(credential_type)
        
        if config_value:
            logger.debug(
                f"Loaded {credential_type} from scraper config",
                extra={
                    "supplier": self.supplier_name,
                    "source": "config"
                }
            )
            return config_value
        
        # Not found in either source
        logger.warning(
            f"Credential not found: {credential_type}",
            extra={
                "supplier": self.supplier_name,
                "credential_type": credential_type,
                "env_key_checked": env_key,
                "config_checked": bool(self.auth_config)
            }
        )
        return None
    
    @staticmethod
    def validate_credentials_available(
        supplier_name: str,
        auth_config: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if credentials are available without raising an exception.
        
        Useful for pre-flight checks before starting expensive operations.
        
        Args:
            supplier_name: Name of the supplier
            auth_config: Optional authentication config dict
            
        Returns:
            Tuple of (credentials_available: bool, error_message: Optional[str])
        """
        try:
            manager = CredentialManager(supplier_name, auth_config)
            manager.get_credentials()
            return True, None
        except CredentialError as e:
            return False, str(e)
