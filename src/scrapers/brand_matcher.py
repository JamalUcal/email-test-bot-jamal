"""
Brand matching utility for web scrapers.

Provides functions to extract, normalize, and match brand names from supplier data
against the brand configuration.
"""

import re
from typing import Optional, List, Dict, Any
from pathlib import Path
import json

from utils.logger import get_logger

logger = get_logger(__name__)

# Cache for brand configs to avoid repeated file reads
_brand_config_cache: Optional[List[Dict[str, Any]]] = None


def extract_brand_from_text(text: str, pattern: str) -> Optional[str]:
    """
    Extract brand name from text using a regex pattern.
    
    Args:
        text: Text to extract brand from
        pattern: Regex pattern to use for extraction
        
    Returns:
        Extracted brand name, or None if no match
    """
    if not text or not pattern:
        return None
    
    try:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            # Return first capture group if exists, otherwise full match
            return match.group(1) if match.groups() else match.group(0)
    except re.error as e:
        logger.error(f"Invalid regex pattern '{pattern}': {e}")
    
    return None


def normalize_brand(brand: str) -> str:
    """
    Normalize brand name for use in filenames.
    
    - Converts to uppercase
    - Replaces non-alphanumeric characters with underscores
    
    Args:
        brand: Brand name to normalize
        
    Returns:
        Normalized brand name
        
    Examples:
        >>> normalize_brand("BMW-OIL")
        'BMW_OIL'
        >>> normalize_brand("vag-oil special")
        'VAG_OIL_SPECIAL'
    """
    if not brand:
        return "UNKNOWN"
    
    # Convert to uppercase
    normalized = brand.upper()
    
    # Replace non-alphanumeric characters with underscores
    normalized = re.sub(r'[^A-Z0-9]+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    return normalized if normalized else "UNKNOWN"


def set_brand_configs_cache(configs: List[Dict[str, Any]]) -> None:
    """
    Pre-populate the brand config cache with already-loaded configs.
    
    Args:
        configs: List of brand configuration dictionaries
    """
    global _brand_config_cache
    _brand_config_cache = configs
    logger.info(f"Brand config cache populated with {len(configs)} configurations")
    # Log sample brands for verification
    sample_brands = [c.get('brand', 'UNKNOWN') for c in configs[:5]]
    logger.info(f"Sample brands in cache: {', '.join(sample_brands)}")


def load_brand_configs(
    config_path: Optional[str] = None,
    use_test_config: bool = False
) -> List[Dict[str, Any]]:
    """
    Load brand configurations from JSON file.
    
    Args:
        config_path: Path to brand_config.json (uses default if None)
        use_test_config: If True, load brand_config_test.json instead
        
    Returns:
        List of brand configuration dictionaries
    """
    global _brand_config_cache
    
    # Return cached config if available
    if _brand_config_cache is not None:
        logger.debug(f"Returning cached brand configs ({len(_brand_config_cache)} brands)")
        return _brand_config_cache
    
    # Determine config path
    if config_path is None:
        # Default to config/brand/brand_config.json (or _test.json) relative to repo root
        repo_root = Path(__file__).parent.parent.parent
        config_filename = "brand_config_test.json" if use_test_config else "brand_config.json"
        config_path = str(repo_root / "config" / "brand" / config_filename)
        logger.debug(f"Loading brand config from: {config_filename}")
    
    try:
        with open(config_path, 'r') as f:
            configs = json.load(f)
        
        _brand_config_cache = configs
        logger.debug(f"Loaded {len(configs)} brand configurations")
        return configs
    
    except FileNotFoundError:
        logger.error(f"Brand config file not found: {config_path}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse brand config JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"Failed to load brand configs: {e}")
        return []


def find_matching_brand(
    extracted_brand: str,
    brand_configs: Optional[List[Dict[str, Any]]] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a matching brand configuration for the extracted brand name.
    
    Matches case-insensitively against both the brand name and aliases.
    
    Args:
        extracted_brand: Brand name extracted from supplier data
        brand_configs: List of brand configurations (loads default if None)
        
    Returns:
        Matching brand config dict, or None if no match found
        
    Examples:
        >>> find_matching_brand("bmw")  # Matches "BMW"
        >>> find_matching_brand("vag")  # Matches alias in config
    """
    if not extracted_brand:
        return None
    
    # Load configs if not provided
    if brand_configs is None:
        brand_configs = load_brand_configs()
    
    if not brand_configs:
        return None
    
    # Normalize for comparison
    extracted_lower = extracted_brand.lower().strip()
    
    for config in brand_configs:
        brand_name = config.get('brand', '')
        
        # Check exact brand name match (case-insensitive)
        if brand_name.lower() == extracted_lower:
            logger.debug(f"Matched brand '{extracted_brand}' to config brand '{brand_name}'")
            return config
        
        # Check aliases
        aliases = config.get('aliases', [])
        if isinstance(aliases, list):
            for alias in aliases:
                if isinstance(alias, str) and alias.lower() == extracted_lower:
                    logger.debug(f"Matched brand '{extracted_brand}' via alias '{alias}' to config brand '{brand_name}'")
                    return config
    
    logger.debug(f"No matching brand config found for '{extracted_brand}'")
    return None


def extract_config_brand(supplier_brand: str, brand_configs: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extract the config brand name from supplier's brand categorization.
    
    Matches supplier's categorized brand (e.g., "VAG-OIL", "BMW_PART1") to the
    base brand name in brand_config.json (e.g., "VAG", "BMW").
    
    Args:
        supplier_brand: Brand from supplier (e.g., "VAG-OIL", "BMW_PART1")
        brand_configs: List of brand configurations from brand_config.json
        
    Returns:
        Config brand name if found, None otherwise
        
    Examples:
        >>> extract_config_brand("VAG-OIL", brand_configs)
        'VAG'
        >>> extract_config_brand("BMW_PART1", brand_configs)
        'BMW'
        >>> extract_config_brand("MERCEDES-BENZ_STOCK", brand_configs)
        'MERCEDES-BENZ'
        >>> extract_config_brand("UNKNOWN", brand_configs)
        None
    """
    if not supplier_brand or not brand_configs:
        return None
    
    # Strategy 1: Try exact match against brand_configs
    exact_match = find_matching_brand(supplier_brand, brand_configs)
    if exact_match:
        return exact_match.get('brand')
    
    # Strategy 2: Try matching prefixes by progressively removing segments from right
    # Handle multiple separators (both - and _)
    # e.g., "BMW_FAST-part1" -> try "BMW_FAST", then "BMW"
    # e.g., "VAG-OIL" -> try "VAG"
    
    test_brand = supplier_brand
    # Keep removing the last segment (separated by - or _) until we find a match
    while True:
        # Find the last separator (either - or _)
        last_sep_pos = max(test_brand.rfind('_'), test_brand.rfind('-'))
        
        if last_sep_pos <= 0:
            # No more separators, we've tried everything
            break
        
        # Remove the last segment and separator
        test_brand = test_brand[:last_sep_pos]
        
        if test_brand:
            match = find_matching_brand(test_brand, brand_configs)
            if match:
                logger.debug(
                    f"Matched supplier brand '{supplier_brand}' to config brand '{match.get('brand')}' via prefix '{test_brand}'"
                )
                return match.get('brand')
    
    # No match found
    logger.debug(f"No config brand found for supplier brand: {supplier_brand}")
    return None


def clear_cache() -> None:
    """Clear the brand config cache. Useful for testing."""
    global _brand_config_cache
    _brand_config_cache = None

