"""
Date parsing from email content.

Parses expiry dates and valid-from dates from email body text
supporting multiple date formats.
"""

import re
from datetime import datetime, timedelta
from typing import Optional, Tuple
from dateutil import parser as dateutil_parser
import pytz

from utils.logger import get_logger
from utils.exceptions import DateParsingError

logger = get_logger(__name__)


class DateParser:
    """Parses dates from email content."""
    
    # Common date-related keywords
    EXPIRY_KEYWORDS = [
        'valid until',
        'expires',
        'expiry',
        'valid through',
        'valid till',
        'effective until',
        'price valid until',
        'prices valid until',
        'valid from',
        'effective from'
    ]
    
    def __init__(self, timezone: str = 'UTC'):
        """
        Initialize date parser.
        
        Args:
            timezone: Timezone for date interpretation (default: UTC)
        """
        self.timezone = pytz.timezone(timezone)
        logger.debug(f"DateParser initialized", timezone=timezone)
    
    def parse_expiry_date(
        self,
        email_body: str,
        email_date: datetime,
        default_days: Optional[int] = None,
        system_default_days: int = 90
    ) -> Tuple[datetime, str, bool]:
        """
        Parse expiry date from email body.
        
        Fallback hierarchy:
        1. Email body
        2. Supplier config default (default_days)
        3. System default duration
        
        Args:
            email_body: Email body text
            email_date: Email received date
            default_days: Default expiry days from supplier config
            system_default_days: System default expiry days
            
        Returns:
            Tuple of (expiry_date, source, is_past_date)
            - expiry_date: Parsed or calculated expiry date
            - source: Where date came from ('body', 'supplier_default', 'system_default')
            - is_past_date: True if date is in the past (warning condition)
        """
        logger.debug(
            "Parsing expiry date",
            has_body=bool(email_body),
            email_date=email_date.isoformat()
        )
        
        # Try to parse from email body
        if email_body:
            parsed_date = self._extract_date_from_text(email_body, email_date)
            if parsed_date:
                is_past = parsed_date < datetime.now(self.timezone)
                logger.info(
                    f"Expiry date parsed from email body: {parsed_date.isoformat()}",
                    is_past=is_past
                )
                return parsed_date, 'body', is_past
        
        # Use supplier default
        if default_days:
            expiry_date = email_date + timedelta(days=default_days)
            logger.info(
                f"Using supplier default expiry: {expiry_date.isoformat()}",
                default_days=default_days
            )
            return expiry_date, 'supplier_default', False
        
        # Use system default
        expiry_date = email_date + timedelta(days=system_default_days)
        logger.info(
            f"Using system default expiry: {expiry_date.isoformat()}",
            system_default_days=system_default_days
        )
        return expiry_date, 'system_default', False
    
    def parse_valid_from_date(
        self,
        email_body: str,
        email_date: datetime
    ) -> datetime:
        """
        Parse valid-from date from email body.
        
        Falls back to email date if not found.
        
        Args:
            email_body: Email body text
            email_date: Email received date
            
        Returns:
            Valid-from date
        """
        logger.debug("Parsing valid-from date")
        
        if email_body:
            # Look for "valid from" or "effective from" patterns
            patterns = [
                r'valid\s+from[:\s]+([^\n]+)',
                r'effective\s+from[:\s]+([^\n]+)',
                r'prices\s+effective[:\s]+([^\n]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, email_body, re.IGNORECASE)
                if match:
                    date_str = match.group(1).strip()
                    parsed_date = self._parse_date_string(date_str, email_date)
                    if parsed_date:
                        logger.info(
                            f"Valid-from date parsed: {parsed_date.isoformat()}",
                            source='body'
                        )
                        return parsed_date
        
        # Default to email date
        logger.info(
            f"Using email date as valid-from: {email_date.isoformat()}",
            source='email_date'
        )
        return email_date
    
    def _extract_date_from_text(
        self,
        text: str,
        reference_date: datetime
    ) -> Optional[datetime]:
        """
        Extract date from text using various patterns.
        
        Args:
            text: Text to search
            reference_date: Reference date for context
            
        Returns:
            Parsed datetime or None
        """
        # Look for date near expiry keywords
        for keyword in self.EXPIRY_KEYWORDS:
            pattern = keyword + r'[:\s]+([^\n.;]+)'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                date_str = match.group(1).strip()
                parsed_date = self._parse_date_string(date_str, reference_date)
                if parsed_date:
                    return parsed_date
        
        # Try to find standalone dates in common formats
        date_patterns = [
            # ISO format: 2025-10-23
            r'\b(\d{4}-\d{2}-\d{2})\b',
            # US format: 10/23/2025 or 10-23-2025
            r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b',
            # European format: 23/10/2025 or 23-10-2025
            r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b',
            # Month name formats: October 23, 2025 or 23 October 2025
            r'\b([A-Za-z]+\s+\d{1,2},?\s+\d{4})\b',
            r'\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b',
            # Short year: 23 Oct 25 or Oct 23, 25
            r'\b([A-Za-z]+\s+\d{1,2},?\s+\d{2})\b',
            r'\b(\d{1,2}\s+[A-Za-z]+\s+\d{2})\b',
            # Month and day only: 23 Oct or Oct 23
            r'\b([A-Za-z]+\s+\d{1,2})\b',
            r'\b(\d{1,2}\s+[A-Za-z]+)\b'
        ]
        
        for pattern in date_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                date_str = match.group(1)
                parsed_date = self._parse_date_string(date_str, reference_date)
                if parsed_date:
                    # Validate it's a reasonable date (within 5 years)
                    days_diff = (parsed_date - reference_date).days
                    if -30 <= days_diff <= 1825:  # -30 days to 5 years
                        return parsed_date
        
        return None
    
    def _parse_date_string(
        self,
        date_str: str,
        reference_date: datetime
    ) -> Optional[datetime]:
        """
        Parse date string using dateutil parser.
        
        Args:
            date_str: Date string to parse
            reference_date: Reference date for context (year inference)
            
        Returns:
            Parsed datetime or None
        """
        try:
            # Clean up the string
            date_str = date_str.strip()
            
            # Remove common trailing text
            date_str = re.sub(r'\s+(onwards?|forward|and\s+beyond).*$', '', date_str, flags=re.IGNORECASE)
            
            # Try parsing with dateutil (fuzzy matching)
            parsed: datetime = dateutil_parser.parse(
                date_str,
                default=reference_date,
                fuzzy=True,
                dayfirst=False  # Prefer MM/DD/YYYY for ambiguous dates
            )
            
            # If year is not in string and parsed year is in past, assume next year
            if str(parsed.year) not in date_str:
                if parsed < reference_date:
                    parsed = parsed.replace(year=reference_date.year + 1)
            
            # Ensure timezone aware
            if parsed.tzinfo is None:
                parsed = self.timezone.localize(parsed)
            
            return parsed
            
        except (ValueError, OverflowError) as e:
            logger.debug(
                f"Failed to parse date string: {date_str}",
                error=str(e)
            )
            return None
    
    def validate_date(
        self,
        date: datetime,
        reference_date: datetime,
        max_future_years: int = 5
    ) -> bool:
        """
        Validate that date is reasonable.
        
        Args:
            date: Date to validate
            reference_date: Reference date
            max_future_years: Maximum years in future allowed
            
        Returns:
            True if date is valid
        """
        days_diff = (date - reference_date).days
        max_days = max_future_years * 365
        
        return -30 <= days_diff <= max_days
