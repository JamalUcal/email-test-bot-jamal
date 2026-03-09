"""
Config merger utility for combining supplier and brand configurations.

Centralizes the logic for merging brand-specific supplier config into brand config,
ensuring consistent handling across email and web scraping workflows.
"""

from typing import Dict, Any, Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)


class ConfigMerger:
    """Handles merging of supplier and brand configurations."""
    
    @staticmethod
    def merge_supplier_brand_config(
        brand: str,
        supplier_config: Dict[str, Any],
        brand_config: Dict[str, Any],
        override_currency: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Merge supplier brand-specific config into brand config.
        
        This combines the brand's base configuration with supplier-specific
        settings like location, currency, and decimal format.
        
        Args:
            brand: Brand name to match (case-insensitive)
            supplier_config: Supplier configuration containing brand-specific settings
            brand_config: Base brand configuration
            override_currency: Optional currency code to override config value
            
        Returns:
            Merged configuration with supplier-specific settings overlaid
            
        Raises:
            ValueError: If no matching brand config found in supplier config
        """
        merged = brand_config.copy()
        
        # Find matching brand config in supplier
        supplier_brand_config = ConfigMerger._find_supplier_brand_config(
            brand=brand,
            supplier_config=supplier_config
        )
        
        # DEBUG: Log what we found
        supplier_name = supplier_config.get('supplier', 'Unknown')
        logger.info(
            f"ConfigMerger: Looking for brand '{brand}' in supplier '{supplier_name}'",
            extra={
                "brand": brand,
                "supplier": supplier_name,
                "supplier_brand_config_found": supplier_brand_config is not None,
                "supplier_brand_config_keys": list(supplier_brand_config.keys()) if supplier_brand_config else None
            }
        )
        
        if not supplier_brand_config:
            raise ValueError(
                f"No configuration found for brand '{brand}' in supplier '{supplier_name}'"
            )
        
        # Merge supplier-specific fields into brand config
        # These fields override brand defaults
        # All parsing uses intelligent header detection (no column config needed)
        merge_fields = ['location', 'currency', 'decimalFormat']
        
        # Get supplier-level defaults (flattened structure - top-level fields)
        # Also check legacy metadata for backwards compatibility
        supplier_metadata = supplier_config.get('metadata', {})
        supplier_defaults = {
            'location': supplier_config.get('location') or supplier_metadata.get('location'),
            'currency': supplier_config.get('currency') or supplier_metadata.get('currency'),
            'decimalFormat': supplier_config.get('decimalFormat') or supplier_metadata.get('decimalFormat')
        }
        
        for field in merge_fields:
            # Use brand-level value if present, otherwise fall back to supplier default
            if field in supplier_brand_config:
                value = supplier_brand_config[field]
            elif field in supplier_defaults and supplier_defaults[field] is not None:
                # Check if field exists AND is not None (allows empty strings, 0, False if needed)
                value = supplier_defaults[field]
                logger.debug(
                    f"Using supplier-level default for {field}",
                    supplier=supplier_name,
                    brand=brand,
                    field=field,
                    value=value
                )
            else:
                # Field not found in either place
                raise ValueError(
                    f"Field '{field}' not found for brand '{brand}' in supplier '{supplier_name}' "
                    f"(must be specified either at brand level or at supplier level)"
                )
            
            # Validate that location and currency are strings, not arrays
            if field in ['location', 'currency']:
                if isinstance(value, list):
                    raise TypeError(
                        f"{field} must be a string, not a list. "
                        f"Found: {value} in {supplier_config.get('supplier')}/{brand}"
                    )
                if not isinstance(value, str):
                    raise TypeError(
                        f"{field} must be a string. "
                        f"Found type: {type(value).__name__} in {supplier_config.get('supplier')}/{brand}"
                    )
            
            merged[field] = value
        
        # Apply override currency if provided (highest priority)
        if override_currency:
            logger.info(
                f"Overriding currency for {brand}: {merged.get('currency')} -> {override_currency}",
                supplier=supplier_name,
                brand=brand,
                original_currency=merged.get('currency'),
                override_currency=override_currency
            )
            merged['currency'] = override_currency
        
        logger.debug(
            f"Merged config for {brand}",
            supplier=supplier_config.get('supplier'),
            brand=brand,
            location=merged.get('location'),
            currency=merged.get('currency')
        )
        
        return merged
    
    @staticmethod
    def _find_supplier_brand_config(
        brand: str,
        supplier_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Find the brand-specific configuration within a supplier config.
        
        Args:
            brand: Brand name to find (case-insensitive)
            supplier_config: Supplier configuration to search
            
        Returns:
            Brand-specific config dict or None if not found
        """
        brand_upper = brand.upper()
        
        for config in supplier_config.get('config', []):
            if config.get('brand', '').upper() == brand_upper:
                return config
        
        return None
    
    @staticmethod
    def get_all_brands_for_supplier(
        supplier_config: Dict[str, Any]
    ) -> List[str]:
        """
        Get list of all brands configured for a supplier.
        
        Args:
            supplier_config: Supplier configuration
            
        Returns:
            List of brand names
        """
        brands: List[str] = []
        
        for config in supplier_config.get('config', []):
            brand = config.get('brand')
            if brand:
                brands.append(brand)
        
        return brands
    
    @staticmethod
    def validate_supplier_config(
        supplier_config: Dict[str, Any]
    ) -> List[str]:
        """
        Validate a supplier configuration for common issues.
        
        Args:
            supplier_config: Supplier configuration to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors: List[str] = []
        supplier_name = supplier_config.get('supplier', 'Unknown')
        
        # Check for config array
        if 'config' not in supplier_config:
            errors.append(f"{supplier_name}: Missing 'config' array")
            return errors
        
        configs = supplier_config.get('config', [])
        if not configs:
            errors.append(f"{supplier_name}: 'config' array is empty")
            return errors
        
        # Validate each brand config
        for idx, config in enumerate(configs):
            brand = config.get('brand', f'config[{idx}]')
            
            # Check required fields
            if 'brand' not in config:
                errors.append(f"{supplier_name}: Missing 'brand' in config[{idx}]")
            
            if 'location' not in config:
                errors.append(f"{supplier_name}/{brand}: Missing 'location'")
            elif isinstance(config['location'], list):
                errors.append(
                    f"{supplier_name}/{brand}: 'location' must be string, not list: {config['location']}"
                )
            
            if 'currency' not in config:
                errors.append(f"{supplier_name}/{brand}: Missing 'currency'")
            elif isinstance(config['currency'], list):
                errors.append(
                    f"{supplier_name}/{brand}: 'currency' must be string, not list: {config['currency']}"
                )
            
            # Note: 'columns' no longer required - using intelligent header detection
        
        return errors

