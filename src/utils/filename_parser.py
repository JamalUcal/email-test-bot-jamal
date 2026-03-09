"""
Filename parser for standard pricing file naming format.

Parses filenames in the format:
    {Brand}_{Supplier}_{Currency}_{Location}_{MMMDD_YYYY}.csv

Examples:
    - VAG_APF_EUR_BELGIUM_SEP18_2025.csv
    - BMW_MATEROM_EUR_ROMANIA_OCT15_2024.csv
    - VAG_OIL_YANXIN_USD_CHINA_JAN05_2026.csv (brand with underscore)
"""

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ParsedFilename:
    """
    Parsed components from a standard pricing filename.
    
    Attributes:
        brand: Brand name (e.g., "VAG", "BMW", "VAG_OIL")
        supplier: Supplier name (e.g., "APF", "MATEROM")
        currency: Currency code (e.g., "EUR", "USD")
        location: Location/country (e.g., "BELGIUM", "ROMANIA")
        valid_from_date: Date extracted from filename
        original_filename: The original filename that was parsed
    """
    brand: str
    supplier: str
    currency: str
    location: str
    valid_from_date: date
    original_filename: str


# Month name mappings for parsing
MONTH_NAMES = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
    'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
    'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
    'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4,
    'JUNE': 6, 'JULY': 7, 'AUGUST': 8,
    'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
}


def parse_standard_filename(filename: str) -> ParsedFilename:
    """
    Parse a standard pricing filename into its components.
    
    Expected format: {Brand}_{Supplier}_{Currency}_{Location}_{MMMDD_YYYY}.csv
    
    The format is parsed from RIGHT to LEFT to handle brands with underscores:
    1. Remove .csv extension
    2. Extract date (last two parts: MMMDD_YYYY)
    3. Extract location (part before date)
    4. Extract currency (3-letter code before location)
    5. Extract supplier (part before currency)
    6. Remaining is brand (may contain underscores)
    
    Args:
        filename: The filename to parse
        
    Returns:
        ParsedFilename with extracted components
        
    Raises:
        ValueError: If filename doesn't match expected format
    
    Examples:
        >>> parse_standard_filename("VAG_APF_EUR_BELGIUM_SEP18_2025.csv")
        ParsedFilename(brand='VAG', supplier='APF', currency='EUR', 
                      location='BELGIUM', valid_from_date=date(2025, 9, 18), ...)
        
        >>> parse_standard_filename("VAG_OIL_YANXIN_USD_CHINA_JAN05_2026.csv")
        ParsedFilename(brand='VAG_OIL', supplier='YANXIN', currency='USD',
                      location='CHINA', valid_from_date=date(2026, 1, 5), ...)
    """
    original_filename = filename
    
    # Remove .csv extension (case-insensitive)
    if filename.lower().endswith('.csv'):
        filename = filename[:-4]
    else:
        raise ValueError(f"Filename must end with .csv: {original_filename}")
    
    # Split by underscore
    parts = filename.split('_')
    
    # Need at least 6 parts: BRAND_SUPPLIER_CURRENCY_LOCATION_MMMDD_YYYY
    # But brand can have underscores, so minimum is 6 parts
    if len(parts) < 6:
        raise ValueError(
            f"Filename has too few parts (expected at least 6): {original_filename}"
        )
    
    try:
        # Parse from right to left
        # Last part is YYYY
        year_str = parts[-1]
        if not year_str.isdigit() or len(year_str) != 4:
            raise ValueError(f"Invalid year format '{year_str}' in {original_filename}")
        year = int(year_str)
        
        # Second to last is MMMDD (e.g., SEP18, OCT05)
        month_day_str = parts[-2].upper()
        month_match = re.match(r'^([A-Z]{3,9})(\d{1,2})$', month_day_str)
        if not month_match:
            raise ValueError(
                f"Invalid month/day format '{parts[-2]}' in {original_filename}. "
                f"Expected format like 'SEP18' or 'OCT05'"
            )
        
        month_name = month_match.group(1)
        day = int(month_match.group(2))
        
        if month_name not in MONTH_NAMES:
            raise ValueError(
                f"Unknown month '{month_name}' in {original_filename}. "
                f"Valid months: {list(MONTH_NAMES.keys())[:12]}"
            )
        
        month = MONTH_NAMES[month_name]
        
        # Validate and create date
        try:
            valid_from_date = date(year, month, day)
        except ValueError as e:
            raise ValueError(
                f"Invalid date in {original_filename}: year={year}, month={month}, day={day}. {e}"
            )
        
        # Third from last is LOCATION
        location = parts[-3].upper()
        if not location:
            raise ValueError(f"Empty location in {original_filename}")
        
        # Fourth from last is CURRENCY (3-letter code)
        currency = parts[-4].upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError(
                f"Invalid currency code '{parts[-4]}' in {original_filename}. "
                f"Expected 3-letter code like 'EUR', 'USD'"
            )
        
        # Fifth from last is SUPPLIER
        supplier = parts[-5].upper()
        if not supplier:
            raise ValueError(f"Empty supplier in {original_filename}")
        
        # Everything before supplier is BRAND (may contain underscores)
        if len(parts) > 6:
            brand = '_'.join(parts[:-5]).upper()
        else:
            brand = parts[0].upper()
        
        if not brand:
            raise ValueError(f"Empty brand in {original_filename}")
        
        result = ParsedFilename(
            brand=brand,
            supplier=supplier,
            currency=currency,
            location=location,
            valid_from_date=valid_from_date,
            original_filename=original_filename
        )
        
        logger.debug(
            f"Parsed filename successfully",
            filename=original_filename,
            brand=brand,
            supplier=supplier,
            currency=currency,
            location=location,
            valid_from_date=valid_from_date.isoformat()
        )
        
        return result
        
    except ValueError:
        # Re-raise ValueError as-is
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse filename '{original_filename}': {str(e)}")


def is_valid_pricing_filename(filename: str) -> bool:
    """
    Check if a filename matches the expected pricing file format.
    
    Args:
        filename: The filename to check
        
    Returns:
        True if the filename can be parsed, False otherwise
    """
    try:
        parse_standard_filename(filename)
        return True
    except ValueError:
        return False


def extract_valid_from_date(filename: str) -> Optional[date]:
    """
    Extract just the valid_from_date from a filename.
    
    This is a convenience function when you only need the date.
    
    Args:
        filename: The filename to parse
        
    Returns:
        The valid_from_date if parseable, None otherwise
    """
    try:
        parsed = parse_standard_filename(filename)
        return parsed.valid_from_date
    except ValueError:
        return None


def generate_standard_filename(
    brand: str,
    supplier: str,
    currency: str,
    location: str,
    valid_from_date: date
) -> str:
    """
    Generate a standard filename from components.
    
    This is the inverse of parse_standard_filename.
    
    Args:
        brand: Brand name
        supplier: Supplier name
        currency: Currency code
        location: Location/country
        valid_from_date: Date for the filename
        
    Returns:
        Formatted filename string
        
    Example:
        >>> generate_standard_filename("VAG", "APF", "EUR", "BELGIUM", date(2025, 9, 18))
        'VAG_APF_EUR_BELGIUM_SEP18_2025.csv'
    """
    # Format date as MMMDD_YYYY
    month_abbr = valid_from_date.strftime('%b').upper()
    day = valid_from_date.strftime('%d')
    year = valid_from_date.strftime('%Y')
    
    # Clean components (uppercase, remove special chars except underscore in brand)
    brand_clean = re.sub(r'[^A-Z0-9_]', '', brand.upper())
    supplier_clean = re.sub(r'[^A-Z0-9]', '', supplier.upper())
    currency_clean = re.sub(r'[^A-Z0-9]', '', currency.upper())
    location_clean = re.sub(r'[^A-Z0-9]', '', location.upper())
    
    return f"{brand_clean}_{supplier_clean}_{currency_clean}_{location_clean}_{month_abbr}{day}_{year}.csv"
