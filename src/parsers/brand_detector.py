"""
Brand name detection from email content.

Detects brand names from filename, subject line, and email body
using configured brand names and aliases.
"""

import re
from typing import Dict, List, Optional, Tuple

from utils.logger import get_logger
from utils.exceptions import BrandDetectionError

logger = get_logger(__name__)


class BrandDetector:
    """Detects brand names from email content."""
    
    def __init__(self, brand_configs: List[Dict]):
        """
        Initialize brand detector.
        
        Args:
            brand_configs: List of brand configurations with names and aliases
        """
        self.brand_configs = brand_configs
        
        # Build lookup map: brand_name/alias -> brand_config
        self.brand_lookup = {}
        for config in brand_configs:
            brand_name = config['brand'].upper()
            self.brand_lookup[brand_name] = config
            
            # Add aliases
            if 'aliases' in config and config['aliases']:
                for alias in config['aliases']:
                    self.brand_lookup[alias.upper()] = config
        
        logger.debug(
            "BrandDetector initialized",
            brands=len(brand_configs),
            total_names=len(self.brand_lookup)
        )
    
    def detect_brand(
        self,
        filename: str,
        subject: str,
        body: Optional[str] = None,
        default_brand: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str], str, bool]:
        """
        Detect brand from email content.
        
        Priority:
        1. Filename
        2. Subject line
        3. Email body (if provided)
        4. Default brand (if provided)
        
        Args:
            filename: Email attachment filename
            subject: Email subject line
            body: Email body text (optional)
            default_brand: Default brand from supplier config (optional)
            
        Returns:
            Tuple of (config_brand, matched_text, source, used_fallback)
            - config_brand: Canonical brand name from config (e.g., "BMW") or None
            - matched_text: Exact text matched in email (e.g., "bmw", "BMW") or None
            - source: Where brand was found ('filename', 'subject', 'body', 'default')
            - used_fallback: True if default brand was used
        """
        logger.debug(
            "Detecting brand",
            filename=filename,
            subject=subject,
            has_body=body is not None,
            has_default=default_brand is not None
        )
        
        # Try filename first
        result = self._find_brand_in_text(filename)
        if result:
            matched_text, config_brand = result
            logger.info(f"Brand detected from filename: {config_brand} (matched: '{matched_text}')", filename=filename)
            return config_brand, matched_text, 'filename', False
        
        # Try subject line
        result = self._find_brand_in_text(subject)
        if result:
            matched_text, config_brand = result
            logger.info(f"Brand detected from subject: {config_brand} (matched: '{matched_text}')", subject=subject)
            return config_brand, matched_text, 'subject', False
        
        # Try body if provided
        if body:
            result = self._find_brand_in_text(body)
            if result:
                matched_text, config_brand = result
                logger.info(f"Brand detected from body: {config_brand} (matched: '{matched_text}')")
                return config_brand, matched_text, 'body', False
        
        # Use default if provided
        if default_brand:
            brand_upper = default_brand.upper()
            if brand_upper in self.brand_lookup:
                logger.warning(
                    f"Using default brand: {default_brand}",
                    filename=filename,
                    subject=subject
                )
                # For default brand, use the provided default as both config and matched text
                return default_brand, default_brand, 'default', True
        
        # No brand found
        logger.warning(
            "Could not detect brand",
            filename=filename,
            subject=subject
        )
        return None, None, 'none', False
    
    def _find_brand_in_text(self, text: str) -> Optional[Tuple[str, str]]:
        """
        Find brand name in text and return both matched text and config brand.
        
        Args:
            text: Text to search
            
        Returns:
            Tuple of (matched_text, config_brand) if found, None otherwise
            - matched_text: The exact text matched in the input (preserves case)
            - config_brand: The canonical brand name from config
        """
        if not text:
            return None
        
        text_upper = text.upper()
        
        # Try exact word matches first (more reliable)
        for brand_name, config in self.brand_lookup.items():
            # Create word boundary pattern
            pattern = r'\b' + re.escape(brand_name) + r'\b'
            match = re.search(pattern, text_upper)
            if match:
                # Extract the matched text from original (non-uppercased) text
                # using the same positions
                start, end = match.span()
                matched_text = text[start:end]
                config_brand: str = config['brand']
                return (matched_text, config_brand)
        
        # Try substring matches (less reliable, only if no word match found)
        for brand_name, config in self.brand_lookup.items():
            index = text_upper.find(brand_name)
            if index != -1:
                # Extract the matched text from original text
                matched_text = text[index:index + len(brand_name)]
                config_brand_substring: str = config['brand']
                return (matched_text, config_brand_substring)
        
        return None
    
    def detect_multiple_brands(
        self,
        filename: str,
        subject: str,
        body: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """
        Detect all brands mentioned in email content.
        
        Useful for detecting if multiple brands are present,
        which may indicate an error condition.
        
        Args:
            filename: Email attachment filename
            subject: Email subject line
            body: Email body text (optional)
            
        Returns:
            List of tuples (matched_text, config_brand) for each detected brand
        """
        brands_found: List[Tuple[str, str]] = []
        seen_config_brands: set[str] = set()
        
        # Check each text source in priority order
        for text_source in [filename, subject, body or ""]:
            if not text_source:
                continue
                
            text_upper = text_source.upper()
            
            # Find all brands in this source
            for brand_name, config in self.brand_lookup.items():
                config_brand = config['brand']
                
                # Skip if we already found this config brand
                if config_brand in seen_config_brands:
                    continue
                
                pattern = r'\b' + re.escape(brand_name) + r'\b'
                match = re.search(pattern, text_upper)
                if match:
                    # Extract matched text preserving case
                    start, end = match.span()
                    matched_text = text_source[start:end]
                    brands_found.append((matched_text, config_brand))
                    seen_config_brands.add(config_brand)
        
        return brands_found
    
    def get_brand_config(self, brand_name: str) -> Optional[Dict]:
        """
        Get brand configuration by name.
        
        Args:
            brand_name: Brand name (case-insensitive)
            
        Returns:
            Brand configuration or None if not found
        """
        brand_upper = brand_name.upper()
        
        if brand_upper in self.brand_lookup:
            return self.brand_lookup[brand_upper]
        
        return None
    
    def validate_brand(self, brand_name: str) -> bool:
        """
        Check if brand name is valid.
        
        Args:
            brand_name: Brand name to validate
            
        Returns:
            True if brand is configured, False otherwise
        """
        return brand_name.upper() in self.brand_lookup
