"""
Version/date detection for web scraper files.

Extracts version identifiers from filenames, metadata, or file content
to support incremental download strategies.
"""

import re
from datetime import datetime
from typing import Dict, Any, Optional

from utils.logger import setup_logger

logger = setup_logger(__name__)


class VersionDetector:
    """Detects version/date identifiers from files or metadata."""
    
    MONTH_PATTERNS = [
        # Format: YYYY-MM or YYYY_MM
        r'(\d{4})[-_](\d{2})',
        # Format: Month name + Year (e.g., "October 2024", "Oct 2024")
        r'(January|February|March|April|May|June|July|August|September|October|November|December|'
        r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s_-]*(\d{4})',
        # Format: Month name alone (e.g., "OCTOBER", "October") - uses current year
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December|'
        r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b(?!\s*\d{4})',
    ]
    
    DATE_PATTERNS = [
        # ISO format: YYYY-MM-DD
        r'(\d{4})-(\d{2})-(\d{2})',
        # Format: YYYYMMDD
        r'(\d{4})(\d{2})(\d{2})',
        # Format: DD-MM-YYYY or DD/MM/YYYY
        r'(\d{2})[-/](\d{2})[-/](\d{4})',
        # Format: MM/DD/YYYY (US format)
        r'(\d{1,2})/(\d{1,2})/(\d{4})',
    ]
    
    DATETIME_PATTERNS = [
        # ISO datetime: YYYY-MM-DDTHH:MM:SS
        r'(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2}):(\d{2})',
        # Format: YYYY-MM-DD HH:MM:SS
        r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})',
        # Format: MM/DD/YYYY HH:MM AM/PM (US format with 12-hour time)
        r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\s+(AM|PM)',
    ]
    
    def detect_version(
        self,
        item: Dict[str, Any],
        detection_mode: str,
        date_field_priority: Optional[list[str]] = None
    ) -> Optional[str]:
        """
        Extract version identifier for comparison.
        
        Args:
            item: Item dictionary (filename, metadata, etc.)
            detection_mode: Detection strategy:
                - "date_based": Extract date/month from filename or metadata
                - "full_scan": No version detection (download all)
            date_field_priority: Optional list of metadata fields to check in order
                                (e.g., ['modified', 'ValidFrom', 'created'])
                
        Returns:
            Version string (normalized to full ISO datetime):
            - "2024-10-01T00:00:00" for month-based (TECHNOPARTS filename)
            - "2024-10-25T00:00:00" for date-based
            - "2024-10-25T10:30:00" for datetime (MATEROM modified)
            - None if no version detectable or full_scan mode
        """
        if detection_mode == "full_scan":
            # No version detection for full scan mode
            return None
        
        if detection_mode != "date_based":
            logger.warning(
                f"Unknown detection mode: {detection_mode}, falling back to date_based"
            )
        
        # Try to extract version from various sources
        version = None
        
        # 1. Try filename
        filename = item.get('filename', '') or item.get('name', '')
        if filename:
            version = self._extract_from_filename(filename)
            if version:
                # Normalize partial dates to full ISO datetime
                version = self._normalize_partial_date(version)
                logger.debug(f"Detected version from filename: {version}", filename=filename)
                return version
        
        # 2. Try metadata fields (use custom priority if provided)
        if date_field_priority:
            metadata_fields = date_field_priority
        else:
            metadata_fields = [
                'ValidFrom', 'valid_from',
                'modified', 'modified_date', 'last_modified',
                'created', 'created_date',
                'date', 'datetime', 'timestamp',
                'version', 'version_date'
            ]
        
        for field in metadata_fields:
            value = item.get(field)
            if value:
                version = self._extract_from_metadata(field, value)
                if version:
                    # Normalize partial dates to full ISO datetime
                    version = self._normalize_partial_date(version)
                    logger.debug(
                        f"Detected version from metadata field: {version}",
                        field=field,
                        value=str(value)
                    )
                    return version
        
        # 3. Try description or title
        for field in ['description', 'title', 'label']:
            value = item.get(field)
            if value:
                version = self._extract_from_text(value)
                if version:
                    # Normalize partial dates to full ISO datetime
                    version = self._normalize_partial_date(version)
                    logger.debug(
                        f"Detected version from {field}: {version}",
                        value=value
                    )
                    return version
        
        logger.debug("No version detected", item_keys=list(item.keys()))
        return None
    
    def is_newer_version(self, new_version: str, old_version: Optional[str]) -> bool:
        """
        Compare versions to determine if new version is newer.
        
        Args:
            new_version: New version string
            old_version: Old version string (or None if no previous version)
            
        Returns:
            True if new_version is newer than old_version
        """
        # If no old version, new is always newer
        if old_version is None:
            return True
        
        # If versions are identical, not newer
        if new_version == old_version:
            return False
        
        # Try to parse as ISO datetime/date for comparison
        try:
            # Normalize to ISO format for comparison
            new_dt = self._parse_version_datetime(new_version)
            old_dt = self._parse_version_datetime(old_version)
            
            if new_dt and old_dt:
                result = new_dt > old_dt
                logger.debug(
                    f"Version comparison: {'newer' if result else 'older/same'}",
                    new_version=new_version,
                    old_version=old_version
                )
                return result
        except Exception as e:
            logger.warning(
                f"Failed to parse version for comparison: {e}",
                new_version=new_version,
                old_version=old_version
            )
        
        # Fallback to string comparison
        result = new_version > old_version
        logger.debug(
            f"Version comparison (string): {'newer' if result else 'older/same'}",
            new_version=new_version,
            old_version=old_version
        )
        return result
    
    def _extract_from_filename(self, filename: str) -> Optional[str]:
        """Extract version from filename."""
        # Try datetime patterns first (most specific)
        for pattern in self.DATETIME_PATTERNS:
            match = re.search(pattern, filename)
            if match:
                return self._format_datetime_match(match)
        
        # Try date patterns
        for pattern in self.DATE_PATTERNS:
            match = re.search(pattern, filename)
            if match:
                return self._format_date_match(match)
        
        # Try month patterns (least specific)
        for pattern in self.MONTH_PATTERNS:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                return self._format_month_match(match)
        
        return None
    
    def _extract_from_metadata(self, field: str, value: Any) -> Optional[str]:
        """Extract version from metadata field."""
        # If value is already a datetime object
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%dT%H:%M:%S')
        
        # If value is a string, try to parse it
        if isinstance(value, str):
            # Try as ISO datetime
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%dT%H:%M:%S')
            except (ValueError, AttributeError):
                pass
            
            # Try as RFC 2822 (WebDAV format: "Mon, 28 Jul 2025 12:21:10 GMT")
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(value)
                return dt.strftime('%Y-%m-%dT%H:%M:%S')
            except (ValueError, AttributeError, TypeError):
                pass
            
            # Try to extract with patterns
            return self._extract_from_text(value)
        
        return None
    
    def _extract_from_text(self, text: str) -> Optional[str]:
        """Extract version from free text."""
        # Try datetime patterns first
        for pattern in self.DATETIME_PATTERNS:
            match = re.search(pattern, text)
            if match:
                return self._format_datetime_match(match)
        
        # Try date patterns
        for pattern in self.DATE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                return self._format_date_match(match)
        
        # Try month patterns
        for pattern in self.MONTH_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._format_month_match(match)
        
        return None
    
    def _format_datetime_match(self, match: re.Match[str]) -> str:
        """Format datetime regex match to ISO string."""
        groups = match.groups()
        
        # Check if this is US format with AM/PM (MM/DD/YYYY HH:MM AM/PM)
        if len(groups) == 6 and groups[5] in ('AM', 'PM'):
            # US format: (month, day, year, hour12, minute, AM/PM)
            month = groups[0].zfill(2)
            day = groups[1].zfill(2)
            year = groups[2]
            hour12 = int(groups[3])
            minute = groups[4]
            ampm = groups[5]
            
            # Convert 12-hour to 24-hour
            if ampm == 'AM':
                hour24 = 0 if hour12 == 12 else hour12
            else:  # PM
                hour24 = 12 if hour12 == 12 else hour12 + 12
            
            hour_str = str(hour24).zfill(2)
            return f"{year}-{month}-{day}T{hour_str}:{minute}:00"
        
        # Standard datetime format (YYYY-MM-DD HH:MM:SS)
        elif len(groups) >= 6:
            return f"{groups[0]}-{groups[1]}-{groups[2]}T{groups[3]}:{groups[4]}:{groups[5]}"
        
        # Date only
        elif len(groups) >= 3:
            return f"{groups[0]}-{groups[1]}-{groups[2]}"
        
        else:
            return match.group(0)
    
    def _format_date_match(self, match: re.Match[str]) -> str:
        """Format date regex match to ISO string."""
        groups = match.groups()
        
        # Ensure we have at least 3 groups
        if len(groups) < 3:
            return match.group(0)
        
        # Check if format is DD-MM-YYYY
        if len(groups[0]) == 2 and len(groups[2]) == 4:
            return f"{groups[2]}-{groups[1]}-{groups[0]}"
        
        # Otherwise YYYY-MM-DD or YYYYMMDD
        return f"{groups[0]}-{groups[1]}-{groups[2]}"
    
    def _format_month_match(self, match: re.Match[str]) -> str:
        """Format month regex match to YYYY-MM string."""
        groups = match.groups()
        
        # Ensure we have at least 1 group
        if len(groups) < 1:
            return match.group(0)
        
        # If first group is numeric (YYYY-MM format)
        if groups[0].isdigit():
            if len(groups) >= 2:
                return f"{groups[0]}-{groups[1]}"
            return match.group(0)
        
        # If first group is month name
        month_name = groups[0]
        
        # Check if we have a year (second group)
        if len(groups) >= 2 and groups[1]:
            year = groups[1]
        else:
            # No year provided, use current year
            year = str(datetime.now().year)
            logger.debug(
                f"Month name without year detected, using current year",
                month=month_name,
                year=year
            )
        
        month_num = self._parse_month_name(month_name)
        return f"{year}-{month_num:02d}"
    
    def _parse_month_name(self, month_name: str) -> int:
        """Parse month name to number (1-12)."""
        months = {
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
        return months.get(month_name.lower(), 1)
    
    def _parse_version_datetime(self, version: str) -> Optional[datetime]:
        """Parse version string to datetime for comparison."""
        # Try ISO datetime
        try:
            return datetime.fromisoformat(version.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            pass
        
        # Try date only (YYYY-MM-DD)
        try:
            return datetime.strptime(version, '%Y-%m-%d')
        except (ValueError, AttributeError):
            pass
        
        # Try month only (YYYY-MM)
        try:
            return datetime.strptime(version, '%Y-%m')
        except (ValueError, AttributeError):
            pass
        
        return None
    
    def _normalize_partial_date(self, date_str: str) -> str:
        """
        Normalize partial date formats to full ISO datetime.
        
        Converts:
        - YYYY-MM -> YYYY-MM-01T00:00:00 (first day of month)
        - YYYY-MM-DD -> YYYY-MM-DDT00:00:00
        - Full ISO datetime -> returns as-is
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Full ISO datetime string
        """
        if not date_str:
            return date_str
        
        # If already full ISO datetime (has 'T' and time component), return as-is
        if 'T' in date_str and ':' in date_str:
            return date_str
        
        # Try to parse and normalize
        try:
            # Try YYYY-MM format (partial date)
            if re.match(r'^\d{4}-\d{2}$', date_str):
                # Add first day of month and time
                return f"{date_str}-01T00:00:00"
            
            # Try YYYY-MM-DD format (date only)
            elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                # Add time component
                return f"{date_str}T00:00:00"
            
            # Try to parse with datetime to validate
            dt = self._parse_version_datetime(date_str)
            if dt:
                return dt.strftime('%Y-%m-%dT%H:%M:%S')
            
        except Exception as e:
            logger.debug(f"Could not normalize date string '{date_str}': {e}")
        
        # Return original if we can't parse it
        return date_str

