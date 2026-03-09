"""
Field name detection for JSON/API data.

Intelligently detects field names in JSON objects/API responses by matching against
configurable variants, eliminating the need for hardcoded field name configurations.

This provides the same DRY field detection approach for JSON/API data that HeaderDetector
provides for Excel/CSV column headers.
"""

import re
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DetectedFields:
    """Result of field name detection."""
    field_mapping: Dict[str, Optional[str]]  # Mapping of logical field names to actual JSON keys (None for optional fields not found)
    missing_required: List[str]  # Required fields not found
    unmatched_keys: List[str]  # JSON keys that didn't match any field
    warnings: List[str]  # Human-readable warnings
    
    def is_valid(self) -> bool:
        """Check if detection was successful (all required fields found)."""
        return len(self.missing_required) == 0


class FieldNameDetector:
    """Detects field names in JSON objects and API responses."""
    
    def __init__(self, column_mapping_config: Dict[str, Any]):
        """
        Initialize field name detector.
        
        Args:
            column_mapping_config: Configuration with field definitions and variants
        """
        self.config = column_mapping_config
        self.fields = column_mapping_config['fields']
        
        # Build normalized variant lookup
        # Maps normalized field name -> (logical_field_name, original_variant)
        self.variant_lookup: Dict[str, tuple[str, str]] = {}
        
        # Store parameterized variants separately for runtime expansion
        # Maps logical_field_name -> list of variants containing <BRAND> or <CURRENCY_CODE>
        self.parameterized_variants: Dict[str, List[str]] = {}
        
        # Store wildcard variants separately for pattern matching
        # Maps logical_field_name -> list of wildcard patterns (e.g., "%price%")
        self.wildcard_variants: Dict[str, List[str]] = {}
        
        # Store exclusion keywords for each field
        # Maps logical_field_name -> list of normalized exclusion keywords
        self.exclusions: Dict[str, List[str]] = {}
        
        for field_name, field_config in self.fields.items():
            for variant in field_config['variants']:
                if '<BRAND>' in variant or '<CURRENCY_CODE>' in variant:
                    # Store parameterized variant for later expansion
                    if field_name not in self.parameterized_variants:
                        self.parameterized_variants[field_name] = []
                    self.parameterized_variants[field_name].append(variant)
                else:
                    # Store regular variant in lookup
                    normalized = self._normalize_text(variant)
                    self.variant_lookup[normalized] = (field_name, variant)
            
            # Store wildcard variants
            if 'wildcard_variants' in field_config:
                self.wildcard_variants[field_name] = field_config['wildcard_variants']
            
            # Store exclusions (normalize for matching)
            if 'exclusions' in field_config:
                self.exclusions[field_name] = [
                    self._normalize_text(ex) for ex in field_config['exclusions']
                ]
        
        logger.debug(
            f"Initialized FieldNameDetector with {len(self.fields)} fields, "
            f"{sum(len(f['variants']) for f in self.fields.values())} total variants"
        )
    
    def _normalize_text(self, text: str) -> str:
        """
        Normalize text for matching.
        
        Converts to uppercase and removes spaces, special characters.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Convert to uppercase
        normalized = text.upper()
        
        # Remove spaces and special characters (keep only alphanumeric)
        normalized = re.sub(r'[^A-Z0-9]', '', normalized)
        
        return normalized
    
    def _matches_wildcard_pattern(self, normalized_field: str, pattern: str) -> bool:
        """
        Check if normalized field name matches a wildcard pattern.
        
        Args:
            normalized_field: Normalized field name to check
            pattern: Wildcard pattern (e.g., "%price%")
            
        Returns:
            True if matches, False otherwise
        """
        # Normalize pattern
        normalized_pattern = self._normalize_text(pattern)
        
        # Convert % to regex .* (match any characters)
        regex_pattern = normalized_pattern.replace('%', '.*')
        
        # Match from start to end
        regex = f'^{regex_pattern}$'
        
        return bool(re.match(regex, normalized_field))
    
    def _check_exclusion(self, normalized_field: str, field_name: str) -> bool:
        """
        Check if field should be excluded based on exclusion keywords.
        
        Args:
            normalized_field: Normalized field name
            field_name: Logical field name to check exclusions for
            
        Returns:
            True if field should be excluded, False otherwise
        """
        if field_name not in self.exclusions:
            return False
        
        for exclusion in self.exclusions[field_name]:
            if exclusion in normalized_field:
                return True
        
        return False
    
    def detect_fields(
        self,
        sample_record: Dict[str, Any],
        matched_brand_text: Optional[str] = None,
        matched_currency_code: Optional[str] = None
    ) -> Dict[str, Optional[str]]:
        """
        Detect field mapping from a sample JSON record.
        
        This is a simplified interface that returns just the field mapping dict,
        suitable for drop-in replacement of hardcoded detection methods.
        
        Args:
            sample_record: Sample JSON record to analyze
            matched_brand_text: Brand name for parameterized variant matching (optional)
            matched_currency_code: Currency code for parameterized variant matching (optional)
            
        Returns:
            Dictionary mapping logical field names to actual JSON keys.
            Optional fields may be None if not found.
        """
        result = self.detect_fields_detailed(
            sample_record,
            matched_brand_text=matched_brand_text,
            matched_currency_code=matched_currency_code
        )
        return result.field_mapping
    
    def detect_fields_detailed(
        self,
        sample_record: Dict[str, Any],
        matched_brand_text: Optional[str] = None,
        matched_currency_code: Optional[str] = None
    ) -> DetectedFields:
        """
        Detect field mapping from a sample JSON record with detailed results.
        
        Args:
            sample_record: Sample JSON record to analyze
            matched_brand_text: Brand name for parameterized variant matching (optional)
            matched_currency_code: Currency code for parameterized variant matching (optional)
            
        Returns:
            DetectedFields with complete detection results
        """
        field_mapping: Dict[str, Optional[str]] = {}
        matched_keys: set[str] = set()
        warnings: List[str] = []
        
        # Try to match each JSON key against variants
        for json_key in sample_record.keys():
            json_key_str = str(json_key).strip()
            if not json_key_str:
                continue
            
            normalized_key = self._normalize_text(json_key_str)
            
            # Try regular variant lookup first (fastest)
            if normalized_key in self.variant_lookup:
                field_name, matched_variant = self.variant_lookup[normalized_key]
                
                # Only record first occurrence of each field
                if field_name not in field_mapping:
                    field_mapping[field_name] = json_key_str
                    matched_keys.add(json_key_str)
                    logger.debug(
                        f"Matched JSON key '{json_key_str}' to field '{field_name}' "
                        f"(variant: '{matched_variant}')"
                    )
                else:
                    warnings.append(
                        f"Duplicate match for field '{field_name}': "
                        f"'{json_key_str}' (already matched to '{field_mapping[field_name]}')"
                    )
                continue
            
            # Try parameterized variants
            matched_param = False
            if matched_brand_text or matched_currency_code:
                for field_name, param_variants in self.parameterized_variants.items():
                    if field_name in field_mapping:
                        continue  # Already found this field
                    
                    for param_variant in param_variants:
                        # Substitute parameters
                        expanded = param_variant
                        if matched_brand_text and '<BRAND>' in expanded:
                            expanded = expanded.replace('<BRAND>', matched_brand_text)
                        if matched_currency_code and '<CURRENCY_CODE>' in expanded:
                            expanded = expanded.replace('<CURRENCY_CODE>', matched_currency_code)
                        
                        normalized_expanded = self._normalize_text(expanded)
                        if normalized_key == normalized_expanded:
                            field_mapping[field_name] = json_key_str
                            matched_keys.add(json_key_str)
                            matched_param = True
                            logger.debug(
                                f"Matched JSON key '{json_key_str}' to field '{field_name}' "
                                f"(parameterized variant: '{param_variant}' -> '{expanded}')"
                            )
                            break
                    
                    if matched_param:
                        break
            
            if matched_param:
                continue
            
            # Try wildcard patterns (last resort)
            matched_wildcard = False
            for field_name, wildcard_patterns in self.wildcard_variants.items():
                if field_name in field_mapping:
                    continue  # Already found this field
                
                # Check exclusions first
                if self._check_exclusion(normalized_key, field_name):
                    logger.debug(
                        f"Excluded JSON key '{json_key_str}' from field '{field_name}' "
                        f"due to exclusion keyword"
                    )
                    continue
                
                for pattern in wildcard_patterns:
                    # Substitute parameters if provided
                    expanded_pattern = pattern
                    if matched_brand_text and '<BRAND>' in expanded_pattern:
                        expanded_pattern = expanded_pattern.replace('<BRAND>', matched_brand_text)
                    if matched_currency_code and '<CURRENCY_CODE>' in expanded_pattern:
                        expanded_pattern = expanded_pattern.replace('<CURRENCY_CODE>', matched_currency_code)
                    
                    if self._matches_wildcard_pattern(normalized_key, expanded_pattern):
                        field_mapping[field_name] = json_key_str
                        matched_keys.add(json_key_str)
                        matched_wildcard = True
                        logger.debug(
                            f"Matched JSON key '{json_key_str}' to field '{field_name}' "
                            f"(wildcard pattern: '{pattern}')"
                        )
                        break
                
                if matched_wildcard:
                    break
        
        # Identify missing required fields
        missing_required: List[str] = []
        for field_name, field_config in self.fields.items():
            if field_config.get('required', False):
                if field_name not in field_mapping:
                    missing_required.append(field_name)
        
        # Identify unmatched keys
        unmatched_keys = [k for k in sample_record.keys() if k not in matched_keys]
        
        # Log summary
        if missing_required:
            # CRITICAL: This is a business problem that MUST be fixed
            # Log all available information for debugging
            all_keys = list(sample_record.keys())
            logger.error(
                f"FIELD DETECTION FAILED: Missing required fields: {missing_required}. "
                f"Actual fields in API response: {all_keys}. "
                f"Matched fields: {field_mapping}. "
                f"Unmatched keys: {unmatched_keys}. "
                f"This is a configuration or API schema issue that requires immediate attention."
            )
        else:
            # Success case - log as info
            logger.info(
                f"Field detection: matched {len(field_mapping)} fields, "
                f"0 required fields missing, "
                f"{len(unmatched_keys)} keys unmatched"
            )
        
        if unmatched_keys and not missing_required:
            # Only debug log unmatched keys if detection was otherwise successful
            logger.debug(f"Unmatched JSON keys: {unmatched_keys[:10]}")
        
        return DetectedFields(
            field_mapping=field_mapping,
            missing_required=missing_required,
            unmatched_keys=unmatched_keys,
            warnings=warnings
        )

