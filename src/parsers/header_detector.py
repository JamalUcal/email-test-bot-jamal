"""
Header detection for price list files.

Intelligently detects column headers by matching against configurable variants,
eliminating the need for hardcoded column position configurations.
"""

import pandas as pd
import re
import math
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path

from utils.logger import get_logger
from utils.exceptions import ParsingError

logger = get_logger(__name__)


@dataclass
class PriceColumnCandidate:
    """A candidate column that might be the price column."""
    col_index: int
    header_text: str
    normalized_header: str
    matched_pattern: str
    rule_priority: int  # Lower = higher priority (order in config)
    matched_currency_code: Optional[str] = None


@dataclass
class DetectedHeaders:
    """Result of header detection."""
    column_indices: Dict[str, int]  # Mapping of field names to 0-based column indices
    header_row_index: int  # Which row contained headers (0-based)
    unrecognized_headers: List[str]  # Headers that didn't match any variant
    missing_required: List[str]  # Required fields not found
    warnings: List[str]  # Human-readable warnings
    matched_currency_code: Optional[str] = None  # Currency code from parameterized header
    
    def is_valid(self) -> bool:
        """Check if detection was successful (all required fields found)."""
        return len(self.missing_required) == 0


class HeaderDetector:
    """Detects column headers in price list files."""
    
    def __init__(self, column_mapping_config: Dict[str, Any]):
        """
        Initialize header detector.
        
        Args:
            column_mapping_config: Configuration with field definitions and variants
        """
        self.config = column_mapping_config
        self.fields = column_mapping_config['fields']
        self.max_blank_rows = column_mapping_config['header_detection'].get('max_blank_rows_to_skip', 10)
        self.price_validation_rows = column_mapping_config['header_detection'].get('price_column_validation_rows', 10)
        
        # Build normalized variant lookup
        # Maps normalized header text -> (field_name, original_variant)
        self.variant_lookup: Dict[str, tuple[str, str]] = {}
        
        # Store parameterized variants separately for runtime expansion
        # Maps field_name -> list of variants containing <BRAND> or <CURRENCY_CODE>
        self.parameterized_variants: Dict[str, List[str]] = {}
        
        # Store wildcard variants separately for pattern matching
        # Maps field_name -> list of wildcard patterns (e.g., "%price%")
        self.wildcard_variants: Dict[str, List[str]] = {}
        
        # Store exclusion keywords for each field
        # Maps field_name -> list of normalized exclusion keywords
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
        
        logger.info(
            f"Initialized HeaderDetector with {len(self.fields)} fields, "
            f"{sum(len(f['variants']) for f in self.fields.values())} total variants "
            f"({sum(len(v) for v in self.parameterized_variants.values())} parameterized)"
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
    
    def _normalize_wildcard_pattern(self, pattern: str) -> str:
        """
        Normalize a wildcard pattern, preserving % as wildcard markers.
        
        Splits on %, normalizes each part, then rejoins with %.
        Example: "%price%" -> "%PRICE%"
        
        Args:
            pattern: Wildcard pattern with % markers
            
        Returns:
            Normalized pattern with % preserved
        """
        parts = pattern.split('%')
        normalized_parts = [self._normalize_text(p) for p in parts]
        return '%'.join(normalized_parts)
    
    def _matches_wildcard_pattern(self, normalized_header: str, pattern: str) -> bool:
        """
        Check if normalized header matches a wildcard pattern.
        
        Args:
            normalized_header: Normalized header text (uppercase, no special chars)
            pattern: Pattern with % wildcards (e.g., "%PRICE%USD%")
            
        Returns:
            True if header matches pattern
        """
        # Convert wildcard pattern to regex
        # Escape any regex special chars except %
        regex_pattern = re.escape(pattern).replace('%', '.*')
        regex_pattern = f'^{regex_pattern}$'
        
        return bool(re.match(regex_pattern, normalized_header))
    
    def _is_excluded(self, normalized_header: str, field_name: str) -> bool:
        """
        Check if a normalized header contains exclusion keywords.
        
        Args:
            normalized_header: Normalized header text
            field_name: Field name to check exclusions for
            
        Returns:
            True if header should be excluded
        """
        if field_name not in self.exclusions:
            return False
        
        for exclusion in self.exclusions[field_name]:
            if exclusion in normalized_header:
                return True
        
        return False
    
    def _is_blank_row(self, row: pd.Series) -> bool:
        """
        Check if a row is blank (all cells empty or null).
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row is blank
        """
        return row.isna().all() or (row.astype(str).str.strip() == '').all()
    
    def _looks_like_price(self, value: Any) -> Tuple[bool, int]:
        """
        Check if a value looks like a valid price.
        
        Args:
            value: Cell value to check
            
        Returns:
            Tuple of (is_price_like, score):
            - is_price_like: True if value could be a price
            - score: +1 for valid price, -2 for alpha chars (strong non-price signal), 0 for empty/null
        """
        # Handle null/empty
        if value is None or (isinstance(value, float) and (pd.isna(value) or math.isnan(value))):
            return False, 0
        
        value_str = str(value).strip()
        if not value_str:
            return False, 0
        
        # Strip Indian Rupee prefixes first (Rs. / Rs) so "Rs.166.00" is treated as numeric
        for prefix in ('Rs.', 'Rs'):
            if value_str.lower().startswith(prefix.lower()):
                value_str = value_str[len(prefix):].strip()
                break
        
        # Remove common currency symbols and prefixes for analysis
        cleaned = re.sub(r'^[\$€£¥₹₽₩R\s]+', '', value_str)
        cleaned = re.sub(r'[\$€£¥₹₽₩R\s]+$', '', cleaned)
        cleaned = cleaned.strip()
        
        if not cleaned:
            return False, 0
        
        # Strong negative signal: contains alphabetic characters (except for 'e' in scientific notation)
        # Part numbers like "001021225NX0" have letters
        alpha_pattern = re.sub(r'[eE][+-]?\d+', '', cleaned)  # Remove scientific notation
        if re.search(r'[a-zA-Z]', alpha_pattern):
            return False, -2
        
        # Try to parse as a number
        try:
            # Handle comma as decimal separator (European format)
            test_value = cleaned.replace(',', '.')
            # Remove thousand separators (but keep decimal)
            test_value = re.sub(r'\.(?=.*\.)', '', test_value)  # Remove all dots except the last
            
            num = float(test_value)
            
            # Reject infinity
            if math.isinf(num):
                return False, -2
            
            # Reject extremely large numbers (likely misinterpreted data like "001021225NX0" -> 1.021225e+96)
            if abs(num) > 1e12:  # 1 trillion - no real price should be this high
                return False, -2
            
            # Reject negative numbers (prices should be positive)
            if num < 0:
                return False, -2
            
            # Looks like a valid price
            return True, 1
            
        except (ValueError, OverflowError):
            return False, -2
    
    def _validate_price_candidates(
        self, 
        candidates: List[PriceColumnCandidate], 
        df_head: pd.DataFrame, 
        header_row_index: int
    ) -> Tuple[PriceColumnCandidate, Optional[str]]:
        """
        Validate multiple price column candidates by checking actual data values.
        
        Scores each candidate by examining the first N data rows after the header.
        Returns the candidate with the highest score, using rule_priority as tie-breaker.
        
        Args:
            candidates: List of price column candidates
            df_head: DataFrame containing header and some data rows
            header_row_index: Row index where header was found (0-based)
            
        Returns:
            Tuple of (best_candidate, currency_code_from_winner)
        """
        if len(candidates) == 1:
            return candidates[0], candidates[0].matched_currency_code
        
        # Score each candidate
        candidate_scores: List[Tuple[PriceColumnCandidate, int]] = []
        
        # Get data rows (rows after header)
        data_start_row = header_row_index + 1
        data_end_row = min(data_start_row + self.price_validation_rows, len(df_head))
        
        logger.info(
            f"Validating {len(candidates)} price column candidates using rows {data_start_row}-{data_end_row-1}"
        )
        
        for candidate in candidates:
            total_score = 0
            valid_count = 0
            invalid_count = 0
            
            for row_idx in range(data_start_row, data_end_row):
                if row_idx >= len(df_head):
                    break
                
                row = df_head.iloc[row_idx]
                if candidate.col_index >= len(row):
                    continue
                
                cell_value = row.iloc[candidate.col_index]
                is_price_like, score = self._looks_like_price(cell_value)
                total_score += score
                
                if score > 0:
                    valid_count += 1
                elif score < 0:
                    invalid_count += 1
            
            candidate_scores.append((candidate, total_score))
            
            logger.info(
                f"  Candidate col {candidate.col_index} '{candidate.header_text}' "
                f"(pattern: '{candidate.matched_pattern}'): "
                f"score={total_score}, valid={valid_count}, invalid={invalid_count}"
            )
        
        # Sort by score (descending), then by rule_priority (ascending for tie-break)
        candidate_scores.sort(key=lambda x: (-x[1], x[0].rule_priority))
        
        best_candidate, best_score = candidate_scores[0]
        
        # Log the decision
        if len(candidate_scores) > 1:
            second_best = candidate_scores[1]
            logger.info(
                f"Selected price column {best_candidate.col_index} '{best_candidate.header_text}' "
                f"(score: {best_score}) over column {second_best[0].col_index} '{second_best[0].header_text}' "
                f"(score: {second_best[1]})"
            )
        
        return best_candidate, best_candidate.matched_currency_code
    
    def _try_detect_headers_in_row(
        self, 
        row: pd.Series, 
        row_index: int,
        matched_brand_text: Optional[str] = None,
        currency_detector: Optional[Any] = None,
        allowed_currencies: Optional[List[str]] = None,
        collect_price_candidates: bool = False
    ) -> Tuple[Optional[DetectedHeaders], List[PriceColumnCandidate]]:
        """
        Attempt to detect headers in a given row.
        
        Args:
            row: Pandas Series representing a row
            row_index: 0-based row index
            matched_brand_text: Brand text matched in email (for <BRAND> substitution)
            currency_detector: CurrencyDetector instance (for <CURRENCY_CODE> substitution)
            allowed_currencies: Optional list of allowed currency codes to restrict detection
            collect_price_candidates: If True, collect ALL matching price columns as candidates
                                     instead of stopping at first match (for later validation)
            
        Returns:
            Tuple of (DetectedHeaders or None, list of price candidates)
            DetectedHeaders is None if not enough required fields found.
            Price candidates list is populated only when collect_price_candidates=True
            and multiple columns match price patterns via wildcard matching.
        """
        column_indices: Dict[str, int] = {}
        unrecognized_headers: List[str] = []
        matched_fields: set[str] = set()
        matched_currency_code: Optional[str] = None
        price_candidates: List[PriceColumnCandidate] = []
        
        # Track rule priority for wildcard patterns (for tie-breaking)
        wildcard_rule_priority = 0
        
        # Try to match each cell against variants
        for col_index, cell_value in enumerate(row):
            if pd.isna(cell_value):
                continue
            
            cell_str = str(cell_value).strip()
            if not cell_str:
                continue
            # Strip BOM and surrounding quotes so cells like "\"MRP\"" or '"MRP"' match "MRP"
            cell_str = cell_str.replace('\ufeff', '').strip()
            cell_str = cell_str.strip('"\'').strip()
            
            normalized_header = self._normalize_text(cell_str)
            
            # Try regular variant lookup first
            if normalized_header in self.variant_lookup:
                field_name, matched_variant = self.variant_lookup[normalized_header]
                
                # Only record first occurrence of each field
                if field_name not in column_indices:
                    column_indices[field_name] = col_index
                    matched_fields.add(field_name)
                    logger.debug(
                        f"Matched header '{cell_str}' (normalized: '{normalized_header}') "
                        f"to field '{field_name}' at column {col_index}"
                    )
                else:
                    logger.warning(
                        f"Duplicate header for field '{field_name}': "
                        f"'{cell_str}' at column {col_index} (already found at column {column_indices[field_name]})"
                    )
            else:
                # Try parameterized variants
                matched_param = False
                if self.parameterized_variants:
                    for field_name, param_variants in self.parameterized_variants.items():
                        # Skip if field already matched
                        if field_name in matched_fields:
                            continue
                            
                        for param_variant in param_variants:
                            # Try <BRAND> substitution
                            if '<BRAND>' in param_variant and matched_brand_text:
                                expanded_variant = param_variant.replace('<BRAND>', matched_brand_text)
                                normalized_expanded = self._normalize_text(expanded_variant)
                                
                                if normalized_header == normalized_expanded:
                                    # Match found!
                                    column_indices[field_name] = col_index
                                    matched_fields.add(field_name)
                                    matched_param = True
                                    logger.debug(
                                        f"Matched header '{cell_str}' via <BRAND> variant "
                                        f"'{param_variant}' -> '{expanded_variant}' "
                                        f"to field '{field_name}' at column {col_index}"
                                    )
                                    break
                            
                            # Try <CURRENCY_CODE> substitution
                            if '<CURRENCY_CODE>' in param_variant and currency_detector:
                                # Use allowed currencies if provided, otherwise all supported
                                currencies_to_check = (
                                    allowed_currencies if allowed_currencies 
                                    else list(currency_detector.supported_currencies.keys())
                                )
                                # Try each currency code
                                for currency_code in currencies_to_check:
                                    expanded_variant = param_variant.replace('<CURRENCY_CODE>', currency_code)
                                    normalized_expanded = self._normalize_text(expanded_variant)
                                    
                                    if normalized_header == normalized_expanded:
                                        # Match found!
                                        column_indices[field_name] = col_index
                                        matched_fields.add(field_name)
                                        matched_param = True
                                        matched_currency_code = currency_code
                                        logger.debug(
                                            f"Matched header '{cell_str}' via <CURRENCY_CODE> variant "
                                            f"'{param_variant}' -> '{expanded_variant}' "
                                            f"to field '{field_name}' at column {col_index}, "
                                            f"detected currency: {currency_code}"
                                        )
                                        break
                                
                                if matched_param:
                                    break
                        
                        if matched_param:
                            break
                
                # Try wildcard pattern matching (last resort fallback)
                if not matched_param and self.wildcard_variants:
                    for field_name, wildcard_patterns in self.wildcard_variants.items():
                        # For price field with collect_price_candidates=True:
                        # Don't skip even if already matched - collect all candidates
                        is_price_field = field_name == 'price'
                        should_collect_candidates = is_price_field and collect_price_candidates
                        
                        # Skip if field already matched (unless collecting price candidates)
                        if field_name in matched_fields and not should_collect_candidates:
                            continue
                        
                        # Check exclusions first (fast rejection)
                        if self._is_excluded(normalized_header, field_name):
                            logger.debug(
                                f"Header '{cell_str}' excluded from '{field_name}' "
                                f"(matches exclusion keyword)"
                            )
                            continue
                        
                        for pattern_idx, wildcard_pattern in enumerate(wildcard_patterns):
                            # Try <CURRENCY_CODE> substitution in wildcard
                            if '<CURRENCY_CODE>' in wildcard_pattern and currency_detector:
                                # Use allowed currencies if provided, otherwise all supported
                                currencies_to_check = (
                                    allowed_currencies if allowed_currencies 
                                    else list(currency_detector.supported_currencies.keys())
                                )
                                for currency_code in currencies_to_check:
                                    expanded_pattern = wildcard_pattern.replace('<CURRENCY_CODE>', currency_code)
                                    normalized_pattern = self._normalize_wildcard_pattern(expanded_pattern)
                                    
                                    if self._matches_wildcard_pattern(normalized_header, normalized_pattern):
                                        # Match found via wildcard!
                                        if should_collect_candidates:
                                            # Collect as candidate for later validation
                                            candidate = PriceColumnCandidate(
                                                col_index=col_index,
                                                header_text=cell_str,
                                                normalized_header=normalized_header,
                                                matched_pattern=wildcard_pattern,
                                                rule_priority=wildcard_rule_priority + pattern_idx,
                                                matched_currency_code=currency_code
                                            )
                                            price_candidates.append(candidate)
                                            logger.debug(
                                                f"Price candidate found: '{cell_str}' via WILDCARD pattern "
                                                f"'{wildcard_pattern}' at column {col_index}"
                                            )
                                            # Mark as matched so it's not added to unrecognized
                                            matched_param = True
                                        else:
                                            column_indices[field_name] = col_index
                                            matched_fields.add(field_name)
                                            matched_param = True
                                            matched_currency_code = currency_code
                                            logger.info(
                                                f"Matched header '{cell_str}' via WILDCARD pattern "
                                                f"'{wildcard_pattern}' (expanded: '{expanded_pattern}') "
                                                f"to field '{field_name}' at column {col_index}, "
                                                f"detected currency: {currency_code}"
                                            )
                                        break
                            else:
                                # Plain wildcard without placeholders
                                normalized_pattern = self._normalize_wildcard_pattern(wildcard_pattern)
                                if self._matches_wildcard_pattern(normalized_header, normalized_pattern):
                                    # Match found via wildcard!
                                    if should_collect_candidates:
                                        # Collect as candidate for later validation
                                        candidate = PriceColumnCandidate(
                                            col_index=col_index,
                                            header_text=cell_str,
                                            normalized_header=normalized_header,
                                            matched_pattern=wildcard_pattern,
                                            rule_priority=wildcard_rule_priority + pattern_idx,
                                            matched_currency_code=None
                                        )
                                        price_candidates.append(candidate)
                                        logger.debug(
                                            f"Price candidate found: '{cell_str}' via WILDCARD pattern "
                                            f"'{wildcard_pattern}' at column {col_index}"
                                        )
                                        # Mark as matched so it's not added to unrecognized
                                        matched_param = True
                                    else:
                                        column_indices[field_name] = col_index
                                        matched_fields.add(field_name)
                                        matched_param = True
                                        logger.info(
                                            f"Matched header '{cell_str}' via WILDCARD pattern "
                                            f"'{wildcard_pattern}' to field '{field_name}' at column {col_index}"
                                        )
                                    break
                            
                            # Don't break out of pattern loop if collecting candidates
                            if matched_param and not should_collect_candidates:
                                break
                        
                        # Don't break out of field loop if collecting candidates
                        if matched_param and not should_collect_candidates:
                            break
                
                # Header didn't match any variant (regular, parameterized, or wildcard)
                if not matched_param and normalized_header:
                    unrecognized_headers.append(cell_str)
                    logger.debug(f"Unrecognized header: '{cell_str}' (normalized: '{normalized_header}')")
        
        # Check if all required fields are present
        # Note: if collecting price candidates, 'price' might not be in matched_fields yet
        required_fields = [
            field_name for field_name, field_config in self.fields.items()
            if field_config.get('required', False)
        ]
        
        # When collecting candidates, count 'price' as found if we have candidates
        effective_matched_fields = set(matched_fields)
        if collect_price_candidates and price_candidates:
            effective_matched_fields.add('price')
        
        missing_required = [f for f in required_fields if f not in effective_matched_fields]
        
        # Log if required fields are missing (but still return result for error reporting)
        if missing_required:
            logger.debug(
                f"Row {row_index} missing required fields: {missing_required}, "
                f"found fields: {list(effective_matched_fields)}"
            )
        
        # Build warnings
        warnings: List[str] = []
        if unrecognized_headers:
            warnings.append(
                f"Unrecognized column headers: {', '.join(unrecognized_headers)}"
            )
        
        return (
            DetectedHeaders(
                column_indices=column_indices,
                header_row_index=row_index,
                unrecognized_headers=unrecognized_headers,
                missing_required=missing_required,
                warnings=warnings,
                matched_currency_code=matched_currency_code
            ),
            price_candidates
        )
    
    def detect_headers(
        self, 
        file_path: str,
        matched_brand_text: Optional[str] = None,
        currency_detector: Optional[Any] = None,
        allowed_currencies: Optional[List[str]] = None
    ) -> DetectedHeaders:
        """
        Detect column headers in a price list file.
        
        Reads first N rows, skips blank rows, and attempts to match headers
        against configured variants.
        
        Args:
            file_path: Path to Excel or CSV file
            matched_brand_text: Brand text matched in email (for <BRAND> substitution)
            currency_detector: CurrencyDetector instance (for <CURRENCY_CODE> substitution)
            allowed_currencies: Optional list of allowed currency codes to restrict detection
            
        Returns:
            DetectedHeaders with column mappings and metadata
            
        Raises:
            ParsingError: If headers cannot be detected or required fields missing
        """
        file_path_obj = Path(file_path)
        
        logger.info(
            f"Detecting headers in: {file_path_obj.name}"
            + (f" (brand: '{matched_brand_text}')" if matched_brand_text else "")
        )
        
        try:
            # Read first N rows (enough to find headers)
            # Try to read as Excel first, fall back to CSV
            try:
                if file_path_obj.suffix.lower() in ['.xlsx', '.xls', '.xlsb']:
                    # .xlsb and .xls require special engines (openpyxl doesn't support them)
                    if file_path_obj.suffix.lower() == '.xlsb':
                        engine = 'pyxlsb'
                    elif file_path_obj.suffix.lower() == '.xls':
                        engine = 'xlrd'
                    else:
                        engine = None
                    df_head = pd.read_excel(
                        file_path,
                        nrows=self.max_blank_rows * 3,  # Read 3x to account for blank rows (logos, etc.)
                        header=None,  # Don't use first row as header automatically
                        engine=engine
                    )
                else:
                    # For .txt and .csv files, auto-detect delimiter
                    # APF files are tab-delimited .txt files
                    df_head = pd.read_csv(
                        file_path,
                        nrows=self.max_blank_rows * 3,  # Read 3x to account for blank rows (logos, etc.)
                        header=None,
                        encoding='utf-8',
                        sep=None,  # Auto-detect delimiter (tab, comma, semicolon, etc.)
                        engine='python'  # Python engine supports sep=None for auto-detection
                    )
            except UnicodeDecodeError:
                # Try different encoding for CSV
                df_head = pd.read_csv(
                    file_path,
                    nrows=self.max_blank_rows * 3,  # Read 3x to account for blank rows (logos, etc.)
                    header=None,
                    encoding='latin-1',
                    sep=None,  # Auto-detect delimiter
                    engine='python'
                )
            
            logger.debug(f"Read {len(df_head)} rows for header detection")
            
            # Track first two non-blank rows for error reporting
            first_rows: List[tuple[int, pd.Series]] = []
            best_result: Optional[DetectedHeaders] = None
            best_result_row_index: Optional[int] = None
            
            # Count non-blank rows processed (not absolute row index)
            # This allows skipping blank rows from logos/images without counting them
            non_blank_rows_checked = 0
            last_row_index = 0
            
            for row_index in range(len(df_head)):
                # Stop after checking max_blank_rows non-blank rows
                if non_blank_rows_checked >= self.max_blank_rows:
                    break
                
                last_row_index = row_index
                row = df_head.iloc[row_index]
                
                # Skip blank rows (don't count toward limit)
                if self._is_blank_row(row):
                    logger.debug(f"Skipping blank row {row_index}")
                    continue
                
                # Count this non-blank row
                non_blank_rows_checked += 1
                
                # Collect first two non-blank rows for error reporting
                if len(first_rows) < 2:
                    first_rows.append((row_index, row))
                
                # Try to detect headers in this row (with price candidate collection)
                result, price_candidates = self._try_detect_headers_in_row(
                    row, row_index, matched_brand_text, currency_detector, allowed_currencies,
                    collect_price_candidates=True  # Always collect to enable smart validation
                )
                
                # Track best result (most fields matched) for error reporting
                if result:
                    if best_result is None or len(result.column_indices) > len(best_result.column_indices):
                        best_result = result
                        best_result_row_index = row_index
                
                if result and result.is_valid():
                    # If we have multiple price candidates, validate them using data
                    if len(price_candidates) > 1:
                        logger.info(
                            f"Multiple price column candidates found ({len(price_candidates)}), "
                            f"validating using data..."
                        )
                        best_candidate, candidate_currency = self._validate_price_candidates(
                            price_candidates, df_head, row_index
                        )
                        # Update result with the validated price column
                        result.column_indices['price'] = best_candidate.col_index
                        if candidate_currency:
                            result.matched_currency_code = candidate_currency
                        logger.info(
                            f"Price column validated: selected column {best_candidate.col_index} "
                            f"'{best_candidate.header_text}'"
                        )
                    elif len(price_candidates) == 1:
                        # Single candidate - use it
                        candidate = price_candidates[0]
                        result.column_indices['price'] = candidate.col_index
                        if candidate.matched_currency_code:
                            result.matched_currency_code = candidate.matched_currency_code
                        logger.info(
                            f"Matched header '{candidate.header_text}' via WILDCARD pattern "
                            f"'{candidate.matched_pattern}' to field 'price' at column {candidate.col_index}"
                        )
                    
                    logger.info(
                        f"Headers detected at row {row_index}: "
                        f"found {len(result.column_indices)} fields "
                        f"({', '.join(result.column_indices.keys())})"
                        + (f", currency: {result.matched_currency_code}" if result.matched_currency_code else "")
                    )
                    
                    # Log unrecognized headers if any
                    if result.unrecognized_headers:
                        logger.debug(
                            f"Unrecognized headers ({len(result.unrecognized_headers)}): "
                            f"{', '.join(result.unrecognized_headers[:5])}"
                            f"{'...' if len(result.unrecognized_headers) > 5 else ''}"
                        )
                    
                    return result
            
            # No valid header row found - build detailed error message with diagnostics
            required_fields = [
                field_name for field_name, field_config in self.fields.items()
                if field_config.get('required', False)
            ]
            
            # Get matched fields from best result
            matched_fields_list: List[str] = []
            missing_required_list: List[str] = required_fields.copy()
            
            if best_result is not None and best_result.column_indices:
                matched_fields_list = list(best_result.column_indices.keys())
                missing_required_list = [f for f in required_fields if f not in matched_fields_list]
            
            # Build comprehensive error message showing first two rows
            error_parts = [
                f"\n{'='*80}",
                f"HEADER DETECTION FAILED: {file_path_obj.name}",
                f"{'='*80}",
                f"\nRequired fields: {', '.join(required_fields)}"
            ]
            
            # Show first two non-blank rows (likely header + first data row)
            if first_rows:
                for i, (row_idx, row_data) in enumerate(first_rows):
                    label = "likely header row" if i == 0 else "first data row"
                    error_parts.append(f"\nROW {row_idx} ({label}):")
                    for col_index, cell_value in enumerate(row_data):
                        if not pd.isna(cell_value):
                            cell_str = str(cell_value).strip()
                            if cell_str:
                                # Truncate long values
                                if len(cell_str) > 50:
                                    cell_str = cell_str[:47] + "..."
                                error_parts.append(f"  Column {col_index}: \"{cell_str}\"")
            
            if matched_fields_list:
                error_parts.append(f"\nSuccessfully matched: {', '.join(matched_fields_list)}")
            
            if missing_required_list:
                error_parts.append(f"Missing required: {', '.join(missing_required_list)}")
            else:
                error_parts.append("Missing required: (none - but headers may be in wrong format)")
            
            error_parts.append(f"\nSearched {non_blank_rows_checked} non-blank rows (up to row {last_row_index}).")
            error_parts.append(f"{'='*80}\n")
            
            error_msg = "\n".join(error_parts)
            logger.error(error_msg)
            raise ParsingError(error_msg)
            
        except Exception as e:
            if isinstance(e, ParsingError):
                raise
            error_msg = f"Failed to detect headers in {file_path_obj.name}: {str(e)}"
            logger.error(error_msg)
            raise ParsingError(error_msg) from e
    
    def get_required_fields(self) -> List[str]:
        """Get list of required field names."""
        return [
            field_name for field_name, field_config in self.fields.items()
            if field_config.get('required', False)
        ]
    
    def get_optional_fields(self) -> List[str]:
        """Get list of optional field names."""
        return [
            field_name for field_name, field_config in self.fields.items()
            if not field_config.get('required', False)
        ]


