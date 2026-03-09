"""
Currency detection from email content and file data.

Detects currency codes from email tags, subject lines, filenames,
and file content using configured currency codes and symbols.
"""

import re
from typing import Dict, List, Optional, Tuple, Any

from utils.logger import get_logger

logger = get_logger(__name__)


class CurrencyDetector:
    """Detects currency codes from various sources."""
    
    def __init__(self, currency_config: Dict[str, Any]):
        """
        Initialize currency detector.
        
        Args:
            currency_config: Currency configuration with supported currencies
        """
        self.currency_config = currency_config
        self.supported_currencies = currency_config['supported_currencies']
        self.normalization = currency_config.get('normalization', {})
        
        # Build lookup maps
        self._build_lookups()
        
        logger.debug(
            "CurrencyDetector initialized",
            supported_currencies=len(self.supported_currencies),
            total_symbols=len(self.symbol_to_code)
        )
    
    def _build_lookups(self) -> None:
        """Build reverse lookup maps for efficient detection."""
        # Map symbol -> currency code
        self.symbol_to_code: Dict[str, str] = {}
        
        # Map normalized alias -> currency code
        self.alias_to_code: Dict[str, str] = {}
        
        for code, currency_info in self.supported_currencies.items():
            # Add symbol mapping
            symbol = currency_info.get('symbol', '')
            if symbol:
                self.symbol_to_code[symbol] = code
            
            # Add alias mappings (normalized)
            for alias in currency_info.get('aliases', []):
                normalized_alias = self._normalize_text(alias)
                self.alias_to_code[normalized_alias] = code
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text for matching according to config.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        if not text or not isinstance(text, str):
            return ""
        
        normalized = text
        
        # Apply normalization rules from config
        if self.normalization.get('case_insensitive', True):
            normalized = normalized.upper()
        
        if self.normalization.get('strip_whitespace', True):
            normalized = normalized.replace(' ', '').replace('\t', '').replace('\n', '')
        
        if self.normalization.get('strip_special_chars', True):
            # Keep only alphanumeric characters
            normalized = re.sub(r'[^A-Z0-9]', '', normalized)
        
        return normalized
    
    def detect_currency_from_tag(self, email_body: Optional[str]) -> Optional[str]:
        """
        Detect currency from CURRENCY: tag in email body.
        
        Args:
            email_body: Email body text
            
        Returns:
            Currency code if found and valid, None otherwise
        """
        if not email_body:
            return None
        
        # Look for CURRENCY: tag (case-insensitive)
        # Pattern: CURRENCY: <currency_code>
        pattern = r'CURRENCY:\s*([A-Z]{3})'
        match = re.search(pattern, email_body, re.IGNORECASE | re.MULTILINE)
        
        if match:
            currency_code = match.group(1).strip().upper()
            
            # Validate against supported currencies
            if currency_code in self.supported_currencies:
                logger.info(
                    f"Found CURRENCY tag in email body: {currency_code}",
                    currency_code=currency_code
                )
                return currency_code
            else:
                logger.warning(
                    f"Found CURRENCY tag with unsupported currency: {currency_code}",
                    currency_code=currency_code,
                    supported=list(self.supported_currencies.keys())
                )
                return None
        
        return None
    
    def detect_currency_from_text(self, text: str) -> Optional[str]:
        """
        Detect currency code from text (subject, filename, etc.).
        
        Searches for 3-letter currency codes in text.
        
        Args:
            text: Text to search (subject line, filename, etc.)
            
        Returns:
            Currency code if found and valid, None otherwise
        """
        if not text:
            return None
        
        normalized_text = self._normalize_text(text)
        
        # Try to find currency codes by checking normalized aliases
        for normalized_alias, currency_code in self.alias_to_code.items():
            if normalized_alias in normalized_text:
                logger.info(
                    f"Currency detected from text: {currency_code}",
                    text=text[:100],  # Log first 100 chars
                    matched_alias=normalized_alias
                )
                return currency_code
        
        return None
    
    def detect_currency_from_text_scoped(
        self, 
        text: str, 
        allowed_currencies: List[str]
    ) -> Optional[str]:
        """
        Detect currency from text, restricted to allowed currencies.
        
        Args:
            text: Text to search
            allowed_currencies: List of currency codes to search for (e.g., ["AED", "USD"])
        
        Returns:
            Currency code if found in allowed list, None otherwise
        """
        if not text or not allowed_currencies:
            return None
        
        normalized_text = self._normalize_text(text)
        
        # Build filtered alias map for only allowed currencies
        filtered_alias_to_code = {}
        for code in allowed_currencies:
            currency_info = self.supported_currencies.get(code.upper())
            if currency_info:
                for alias in currency_info.get('aliases', []):
                    normalized_alias = self._normalize_text(alias)
                    filtered_alias_to_code[normalized_alias] = code.upper()
        
        # Search text with filtered aliases
        for normalized_alias, currency_code in filtered_alias_to_code.items():
            if normalized_alias in normalized_text:
                logger.info(
                    f"Currency detected from text (scoped): {currency_code}",
                    text=text[:100],
                    matched_alias=normalized_alias,
                    allowed_currencies=allowed_currencies
                )
                return currency_code
        
        return None
    
    def detect_currency_from_symbol(self, text: str) -> Optional[str]:
        """
        Detect currency from symbol in text (e.g., $, €, £).
        
        Args:
            text: Text containing potential currency symbol
            
        Returns:
            Currency code if symbol found and mapped, None otherwise
        """
        if not text:
            logger.debug("[CURRENCY DEBUG] detect_currency_from_symbol called with empty text")
            return None
        
        logger.info(
            f"[CURRENCY DEBUG] detect_currency_from_symbol called",
            text_sample=repr(text[:100]),
            text_length=len(text),
            symbol_to_code_map=self.symbol_to_code
        )
        
        # Check each known symbol
        for symbol, currency_code in self.symbol_to_code.items():
            logger.debug(
                f"[CURRENCY DEBUG] Checking symbol: {repr(symbol)} -> {currency_code}",
                symbol_in_text=(symbol in text)
            )
            if symbol in text:
                logger.info(
                    f"[CURRENCY DEBUG] ✓ Currency symbol MATCHED: {repr(symbol)} -> {currency_code}",
                    text=text[:50]
                )
                return currency_code
        
        logger.info(
            f"[CURRENCY DEBUG] ✗ No currency symbol matched",
            text_sample=repr(text[:100]),
            available_symbols=list(self.symbol_to_code.keys())
        )
        return None
    
    def detect_currency_from_symbol_scoped(
        self, 
        symbol: str, 
        allowed_currencies: List[str]
    ) -> Optional[str]:
        """
        Detect currency from symbol, scoped to allowed currencies.
        
        When a symbol maps to multiple currencies (e.g., $ → USD, SGD),
        this method finds ALL currencies with that symbol and returns the
        first one that's in the allowed_currencies list.
        
        Args:
            symbol: Currency symbol (e.g., "$", "€")
            allowed_currencies: List of allowed currency codes (e.g., ["USD", "EUR"])
            
        Returns:
            Currency code if symbol found and in allowed list, None otherwise
        """
        if not symbol or not allowed_currencies:
            return None
        
        # Find all currencies that use this symbol
        matching_currencies = []
        for currency_code, currency_info in self.supported_currencies.items():
            if currency_info.get('symbol') == symbol:
                matching_currencies.append(currency_code)
        
        logger.info(
            f"[CURRENCY DEBUG] Symbol '{symbol}' matches currencies: {matching_currencies}",
            symbol=symbol,
            matching_currencies=matching_currencies,
            allowed_currencies=allowed_currencies
        )
        
        # Filter to only allowed currencies
        for currency_code in matching_currencies:
            if currency_code in allowed_currencies:
                logger.info(
                    f"[CURRENCY DEBUG] ✓ Scoped symbol match: '{symbol}' -> {currency_code}",
                    symbol=symbol,
                    currency=currency_code,
                    reason="in_allowed_currencies"
                )
                return currency_code
        
        logger.info(
            f"[CURRENCY DEBUG] ✗ No scoped match for symbol '{symbol}'",
            symbol=symbol,
            matching_currencies=matching_currencies,
            allowed_currencies=allowed_currencies
        )
        return None
    
    def is_currency_ambiguous(self, supplier_config: Dict[str, Any], brand: str) -> bool:
        """
        Check if supplier has multiple currencies for the same brand.
        
        Args:
            supplier_config: Supplier configuration
            brand: Brand name (case-insensitive)
            
        Returns:
            True if supplier has >1 currency for this brand
        """
        currencies = self.get_supplier_currencies_for_brand(supplier_config, brand)
        is_ambiguous = len(currencies) > 1
        
        if is_ambiguous:
            logger.info(
                f"Currency ambiguous for brand {brand}",
                supplier=supplier_config.get('supplier'),
                brand=brand,
                currencies=currencies
            )
        
        return is_ambiguous
    
    def get_supplier_currencies_for_brand(
        self, 
        supplier_config: Dict[str, Any], 
        brand: str
    ) -> List[str]:
        """
        Get all currency codes for a specific brand in supplier config.
        
        Args:
            supplier_config: Supplier configuration
            brand: Brand name (case-insensitive)
            
        Returns:
            List of currency codes for this brand
        """
        currencies: List[str] = []
        brand_upper = brand.upper()
        
        # Check brand configs within supplier
        brand_configs = supplier_config.get('config', [])
        
        for brand_config in brand_configs:
            config_brand = brand_config.get('brand', '').upper()
            if config_brand == brand_upper:
                currency = brand_config.get('currency')
                if currency and currency not in currencies:
                    currencies.append(currency)
        
        logger.debug(
            f"Found {len(currencies)} currency(ies) for brand {brand}",
            supplier=supplier_config.get('supplier'),
            brand=brand,
            currencies=currencies
        )
        
        return currencies
    
    def validate_currency(self, currency_code: str) -> bool:
        """
        Validate if currency code is supported.
        
        Args:
            currency_code: Currency code to validate
            
        Returns:
            True if supported, False otherwise
        """
        return currency_code.upper() in self.supported_currencies
    
    def get_currency_info(self, currency_code: str) -> Optional[Dict[str, Any]]:
        """
        Get full currency information.
        
        Args:
            currency_code: Currency code
            
        Returns:
            Currency info dict or None if not found
        """
        return self.supported_currencies.get(currency_code.upper())

