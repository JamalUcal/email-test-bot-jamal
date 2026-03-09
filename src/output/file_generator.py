"""
File generator for creating CSV output files.

Generates properly formatted CSV files from parsed pricing data,
applying transformations like GST removal, discounts, and part number padding.

Supports BigQuery-based supersession reconciliation when enabled.
"""

import csv
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING
from pathlib import Path
from decimal import Decimal, InvalidOperation

from parsers.price_list_parser import ParsedPriceList, PriceListItem
from parsers.header_detector import DetectedHeaders
from utils.logger import get_logger
from utils.exceptions import FileGenerationError

if TYPE_CHECKING:
    from storage.bigquery_processor import BigQueryPriceListProcessor

logger = get_logger(__name__)


class FileGenerator:
    """Generates CSV output files from parsed price lists."""
    
    # Default error rate threshold (2%)
    DEFAULT_ERROR_RATE_THRESHOLD = 0.02
    
    def __init__(self, column_mapping_config: Optional[Dict[str, Any]] = None):
        """
        Initialize file generator.
        
        Args:
            column_mapping_config: Optional config with parsing.error_rate_warning_threshold
        """
        # Extract error rate warning threshold from config
        self.error_rate_threshold = self.DEFAULT_ERROR_RATE_THRESHOLD
        if column_mapping_config:
            parsing_config = column_mapping_config.get('parsing', {})
            self.error_rate_threshold = parsing_config.get(
                'error_rate_warning_threshold', 
                self.DEFAULT_ERROR_RATE_THRESHOLD
            )
    
    def generate_csv_streaming(
        self,
        input_file_path: str,
        parser: Any,  # PriceListParser instance
        brand_config: Dict[str, Any],
        supplier_config: Dict[str, Any],
        valid_from_date: datetime,
        output_path: str,
        supplier_brand: Optional[str] = None,
        matched_brand_text: Optional[str] = None,
        currency_detector: Optional[Any] = None,
        detected_headers: Optional[DetectedHeaders] = None
    ) -> Tuple[str, int, int, List[str]]:
        """
        Generate CSV by streaming directly from input file (no memory accumulation).
        
        Args:
            input_file_path: Path to input Excel/CSV file
            parser: PriceListParser instance
            brand_config: Brand configuration
            supplier_config: Supplier configuration
            valid_from_date: Valid from date for filename
            output_path: Directory path for output file
            supplier_brand: Supplier's original brand categorization (e.g., "BMW_PART1").
                          If provided, preserves supplier's categorization in filename.
                          If None, uses brand from brand_config.
            matched_brand_text: Brand text matched in email (for <BRAND> substitution)
            currency_detector: CurrencyDetector instance (for <CURRENCY_CODE> substitution)
            detected_headers: Optional pre-detected headers to skip redundant header parsing
            
        Returns:
            Tuple of (output_file_path, total_rows, valid_rows, warnings)
        """
        try:
            warnings: List[str] = []
            
            # Generate filename
            # For web scrapers: use supplier's original categorization (e.g., "BMW_PART1")
            # For email processing: use brand from brand_config (e.g., "BMW")
            brand = supplier_brand if supplier_brand else brand_config.get('brand')
            if not brand:
                raise ValueError("Brand name is required")
            supplier = supplier_config.get('supplier')
            if not supplier:
                raise ValueError("Supplier name is required in supplier config")
            location = brand_config.get('location', 'Unknown')
            currency = brand_config.get('currency', 'Unknown')
            
            filename = self._generate_filename(
                brand=brand,
                supplier=supplier,
                location=location,
                currency=currency,
                valid_from_date=valid_from_date
            )
            
            # Prepare output file
            output_file = Path(output_path) / filename
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(
                "Starting streaming CSV generation",
                input_file=input_file_path,
                output_file=str(output_file)
            )
            
            # Open CSV for writing
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'Brand',
                    'Supplier Name',
                    'Location',
                    'Currency',
                    'Part Number',
                    'Description',
                    'FORMER PN',
                    'SUPERSESSION',
                    'Price'
                ])
                
                # Create transform function (closure with config)
                def transform_item(item: PriceListItem) -> Dict[str, Any]:
                    return self._transform_item(
                        item=item,
                        brand_config=brand_config,
                        supplier_config=supplier_config,
                        parsed_list=None,  # Not needed for streaming
                        valid_from_date=valid_from_date
                    )
                
                # Stream parse and write directly to CSV
                total_rows: int
                valid_rows: int
                errors: List[str]
                total_rows, valid_rows, errors = parser.stream_parse_to_csv(
                    file_path=input_file_path,
                    brand_config=brand_config,
                    csv_writer=writer,
                    transform_func=transform_item,
                    matched_brand_text=matched_brand_text,
                    currency_detector=currency_detector,
                    detected_headers=detected_headers  # Pass cached headers to skip re-detection
                )
            
            logger.info(
                f"Streaming CSV generation complete: {valid_rows}/{total_rows} rows",
                output_file=str(output_file),
                total_rows=total_rows,
                valid_rows=valid_rows,
                errors=len(errors)
            )
            
            # Check for high error rate
            if total_rows > 0:
                error_count = total_rows - valid_rows
                error_rate = error_count / total_rows
                if error_rate > self.error_rate_threshold:
                    error_pct = error_rate * 100
                    threshold_pct = self.error_rate_threshold * 100
                    warning_msg = (
                        f"HIGH PARSING ERROR RATE: {error_pct:.1f}% "
                        f"({error_count:,}/{total_rows:,} rows failed) - "
                        f"exceeds {threshold_pct:.0f}% threshold. "
                        f"Check if correct price column was detected."
                    )
                    warnings.insert(0, warning_msg)  # Add at beginning for visibility
                    logger.warning(
                        warning_msg,
                        error_rate=error_rate,
                        error_count=error_count,
                        total_rows=total_rows,
                        threshold=self.error_rate_threshold
                    )
            
            # Convert errors to warnings
            if errors:
                warnings.extend(errors[:10])  # Limit to first 10 errors
                if len(errors) > 10:
                    warnings.append(f"... and {len(errors) - 10} more errors")
            
            return str(output_file), total_rows, valid_rows, warnings
            
        except Exception as e:
            error_msg = f"Failed to generate CSV: {str(e)}"
            logger.error(error_msg, error=str(e))
            raise FileGenerationError(error_msg)
    
    def generate_csv_with_bigquery_reconciliation(
        self,
        input_file_path: str,
        parser: Any,  # PriceListParser instance
        brand_config: Dict[str, Any],
        supplier_config: Dict[str, Any],
        valid_from_date: datetime,
        output_path: str,
        bq_processor: 'BigQueryPriceListProcessor',
        supplier_brand: Optional[str] = None,
        matched_brand_text: Optional[str] = None,
        currency_detector: Optional[Any] = None,
        detected_headers: Optional[DetectedHeaders] = None,
        source_email_subject: Optional[str] = None,
        source_email_date: Optional[datetime] = None
    ) -> Tuple[str, int, int, List[str], Dict[str, Any]]:
        """
        Generate CSV with BigQuery-based supersession reconciliation.
        
        Memory-efficient flow:
        1. Use existing streaming parser → local CSV (no change to current flow)
        2. Upload local CSV → GCS → BigQuery load job
        3. Run reconciliation stored procedure
        4. Export reconciled data → GCS
        5. Download to temp file for Drive upload
        
        Peak memory stays at ~100MB regardless of file size.
        
        Args:
            input_file_path: Path to input Excel/CSV file
            parser: PriceListParser instance
            brand_config: Brand configuration
            supplier_config: Supplier configuration
            valid_from_date: Valid from date for filename
            output_path: Directory path for output file
            bq_processor: BigQueryPriceListProcessor instance
            supplier_brand: Supplier's original brand categorization
            matched_brand_text: Brand text matched in email
            currency_detector: CurrencyDetector instance
            detected_headers: Optional pre-detected headers
            source_email_subject: Email subject (for tracking in BigQuery)
            source_email_date: Email date (for tracking in BigQuery)
            
        Returns:
            Tuple of (output_file_path, total_rows, valid_rows, warnings, reconciliation_info)
            
            reconciliation_info contains:
            - price_list_id: UUID of the price list in BigQuery
            - stats: Reconciliation statistics
            - errors: List of reconciliation errors
        """
        try:
            warnings: List[str] = []
            reconciliation_info: Dict[str, Any] = {}
            
            # Step 1: Generate initial CSV using existing streaming pipeline
            # This creates a local temp CSV file
            local_csv_path, total_rows, valid_rows, parse_warnings = self.generate_csv_streaming(
                input_file_path=input_file_path,
                parser=parser,
                brand_config=brand_config,
                supplier_config=supplier_config,
                valid_from_date=valid_from_date,
                output_path=output_path,
                supplier_brand=supplier_brand,
                matched_brand_text=matched_brand_text,
                currency_detector=currency_detector,
                detected_headers=detected_headers
            )
            warnings.extend(parse_warnings)
            
            logger.info(
                "Initial CSV generated, starting BigQuery reconciliation",
                local_csv_path=local_csv_path,
                total_rows=total_rows,
                valid_rows=valid_rows
            )
            
            # Step 2: Process through BigQuery (GCS intermediary)
            # This uploads to GCS, loads to BigQuery, reconciles, exports back to GCS
            brand = supplier_brand if supplier_brand else brand_config.get('brand')
            supplier = supplier_config.get('supplier')
            currency = brand_config.get('currency', 'Unknown')
            location = brand_config.get('location', 'Unknown')
            
            # Validate required fields
            if not brand:
                raise FileGenerationError("Brand name is required for BigQuery reconciliation")
            if not supplier:
                raise FileGenerationError("Supplier name is required for BigQuery reconciliation")
            
            price_list_id, gcs_reconciled_path = bq_processor.process_price_list(
                local_csv_path=local_csv_path,
                supplier=supplier,
                brand=brand,
                currency=currency,
                location=location,
                source_filename=Path(input_file_path).name,
                valid_from_date=valid_from_date.date() if hasattr(valid_from_date, 'date') else valid_from_date,
                source_email_subject=source_email_subject,
                source_email_date=source_email_date
            )
            
            reconciliation_info['price_list_id'] = price_list_id
            
            # Step 3: Get reconciliation stats and errors
            stats = bq_processor.get_reconciliation_stats(price_list_id)
            reconciliation_info['stats'] = stats
            
            # Update row counts from BigQuery (may have changed due to synthetic rows)
            if stats.get('total_items'):
                total_rows = stats['total_items']
            if stats.get('reconciled_items'):
                valid_rows = stats['reconciled_items']
            
            # Log stats
            logger.info(
                "BigQuery reconciliation completed",
                price_list_id=price_list_id,
                total_items=stats.get('total_items'),
                reconciled_items=stats.get('reconciled_items'),
                duplicates_found=stats.get('duplicates_found'),
                duplicates_removed=stats.get('duplicates_removed'),
                synthetic_items_added=stats.get('synthetic_items_added'),
                items_with_errors=stats.get('items_with_errors')
            )
            
            # Add reconciliation stats to warnings for visibility
            if stats.get('duplicates_found', 0) > 0:
                warnings.append(
                    f"De-duplication: {stats.get('duplicates_found')} duplicate part numbers found, "
                    f"{stats.get('duplicates_removed')} rows removed"
                )
            
            if stats.get('synthetic_items_added', 0) > 0:
                warnings.append(
                    f"Supersession: {stats.get('synthetic_items_added')} synthetic rows added for missing supersessions"
                )
            
            # Get reconciliation errors (circular refs, etc.)
            recon_errors = bq_processor.get_reconciliation_errors(price_list_id)
            reconciliation_info['errors'] = recon_errors
            
            if recon_errors:
                for err in recon_errors[:5]:  # Limit to first 5
                    warnings.append(
                        f"Supersession error for {err.get('part_number')}: "
                        f"{err.get('reconciliation_status')} - {err.get('reconciliation_error_message', 'Unknown error')}"
                    )
                if len(recon_errors) > 5:
                    warnings.append(f"... and {len(recon_errors) - 5} more supersession errors")
            
            # Get processing errors (duplicates with conflicts, etc.)
            processing_errors = bq_processor.get_processing_errors(price_list_id)
            reconciliation_info['processing_errors'] = processing_errors
            
            # Step 4: Download reconciled CSV from GCS to temp file
            reconciled_csv_path = bq_processor.download_reconciled_to_temp(gcs_reconciled_path)
            
            # Step 5: Replace the original local CSV with the reconciled one
            # The reconciled file will be uploaded to Drive by the orchestrator
            # Delete the original unreconciled CSV
            try:
                os.remove(local_csv_path)
            except OSError:
                pass  # Best effort
            
            # Generate the proper filename for the reconciled file
            filename = self._generate_filename(
                brand=brand,
                supplier=supplier,
                location=location,
                currency=currency,
                valid_from_date=valid_from_date
            )
            
            # Move reconciled file to output path with proper name
            final_output_path = Path(output_path) / filename
            final_output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy reconciled file to final location (can't just rename as temp might be on different filesystem)
            import shutil
            shutil.move(reconciled_csv_path, str(final_output_path))
            
            logger.info(
                "BigQuery reconciliation complete, reconciled CSV ready",
                output_file=str(final_output_path),
                price_list_id=price_list_id
            )
            
            return str(final_output_path), total_rows, valid_rows, warnings, reconciliation_info
            
        except Exception as e:
            error_msg = f"Failed to generate CSV with BigQuery reconciliation: {str(e)}"
            logger.error(error_msg, error=str(e))
            raise FileGenerationError(error_msg)
    
    def generate_csv(
        self,
        parsed_list: ParsedPriceList,
        brand_config: Dict[str, Any],
        supplier_config: Dict[str, Any],
        valid_from_date: datetime,
        output_path: str
    ) -> Tuple[str, List[str]]:
        """
        Generate CSV file from parsed price list.
        
        Args:
            parsed_list: Parsed price list data
            brand_config: Brand configuration with minimumPartLength
            supplier_config: Supplier configuration with discount_percent
            valid_from_date: Valid from date for filename
            output_path: Directory path for output file
            
        Returns:
            Tuple of (output_file_path, list of warnings)
            
        Raises:
            FileGenerationError: If file generation fails
        """
        try:
            warnings: List[str] = []
            
            logger.info(
                "Generating CSV file",
                supplier=parsed_list.supplier,
                brand=parsed_list.brand,
                items=len(parsed_list.items)
            )
            
            # Generate filename
            filename = self._generate_filename(
                brand=parsed_list.brand,
                supplier=parsed_list.supplier,
                location=parsed_list.location,
                currency=parsed_list.currency,
                valid_from_date=valid_from_date
            )
            
            # Prepare output file
            output_file = Path(output_path) / filename
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Stream write to CSV (no memory accumulation)
            rows_written = 0
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'Brand',
                    'Supplier Name',
                    'Location',
                    'Currency',
                    'Part Number',
                    'Description',
                    'FORMER PN',
                    'SUPERSESSION',
                    'Price'
                ])
                
                # Stream process and write each item
                for item in parsed_list.items:
                    try:
                        # Transform item
                        transformed_item = self._transform_item(
                            item=item,
                            brand_config=brand_config,
                            supplier_config=supplier_config,
                            parsed_list=parsed_list,
                            valid_from_date=valid_from_date
                        )
                        
                        # Write row immediately (not stored in memory)
                        writer.writerow([
                            transformed_item['Brand'],
                            transformed_item['Supplier Name'],
                            transformed_item['Location'],
                            transformed_item['Currency'],
                            transformed_item['Part Number'],
                            transformed_item['Description'],
                            transformed_item['FORMER PN'],
                            transformed_item['SUPERSESSION'],
                            transformed_item['Price']
                        ])
                        
                        rows_written += 1
                        
                        # Log progress for large files
                        if rows_written % 10000 == 0:
                            logger.info(
                                f"Written {rows_written} rows to CSV",
                                rows_written=rows_written
                            )
                        
                    except Exception as e:
                        warning = f"Row {item.row_number}: Failed to transform item - {str(e)}"
                        warnings.append(warning)
                        logger.warning(warning, row=item.row_number, error=str(e))
            
            if rows_written == 0:
                raise FileGenerationError("No valid items written to CSV")
            
            logger.info(
                "CSV file generated successfully",
                filename=filename,
                rows=rows_written,
                warnings=len(warnings)
            )
            
            return str(output_file), warnings
            
        except Exception as e:
            error_msg = f"Failed to generate CSV: {str(e)}"
            logger.error(error_msg, error=str(e))
            raise FileGenerationError(error_msg)
    
    def _generate_filename(
        self,
        brand: str,
        supplier: str,
        location: str,
        currency: str,
        valid_from_date: datetime
    ) -> str:
        """
        Generate output filename.
        
        Format: Brand_SupplierName_Location_Currency_ValidFromDateMMYY.csv
        Example: VAG_APF_EUR_BELGIUM_SEP18_2025.csv
        
        Args:
            brand: Brand name
            supplier: Supplier name
            location: Location
            currency: Currency code
            valid_from_date: Valid from date
            
        Returns:
            Formatted filename
        """
        # Format date as MMMDD_YYYY (e.g., SEP18_2025)
        month_abbr = valid_from_date.strftime('%b').upper()
        day = valid_from_date.strftime('%d')
        year = valid_from_date.strftime('%Y')
        date_str = f"{month_abbr}{day}_{year}"
        
        # Clean components (remove special characters, convert to uppercase)
        # Note: Preserve underscores in brand names (e.g., "VAG_OIL" should stay "VAG_OIL")
        brand_clean = re.sub(r'[^A-Z0-9_]', '', brand.upper())
        supplier_clean = re.sub(r'[^A-Z0-9]', '', supplier.upper())
        location_clean = re.sub(r'[^A-Z0-9]', '', location.upper())
        currency_clean = re.sub(r'[^A-Z0-9]', '', currency.upper())
        
        filename = f"{brand_clean}_{supplier_clean}_{currency_clean}_{location_clean}_{date_str}.csv"
        
        logger.debug(f"Generated filename: {filename}")
        
        return filename
    
    def _transform_item(
        self,
        item: PriceListItem,
        brand_config: Dict[str, Any],
        supplier_config: Dict[str, Any],
        parsed_list: Optional[ParsedPriceList],
        valid_from_date: datetime
    ) -> Dict[str, Any]:
        """
        Transform a single price list item.
        
        Applies:
        1. Part number padding
        2. GST removal (if applicable)
        3. Discount application
        
        Args:
            item: Price list item
            brand_config: Brand configuration
            supplier_config: Supplier configuration
            parsed_list: Full parsed list for metadata
            valid_from_date: Valid from date
            
        Returns:
            Transformed item dictionary
        """
        # Format date for Supplier Name field
        month_abbr = valid_from_date.strftime('%b').upper()
        year = valid_from_date.strftime('%Y')
        
        # Build Supplier Name: SupplierName_Currency_Month_Year_Location
        if parsed_list is not None:
            supplier_name = f"{parsed_list.supplier}_{parsed_list.currency}_{month_abbr}_{year}_{parsed_list.location}"
            brand = parsed_list.brand
            location = parsed_list.location
            currency = parsed_list.currency
        else:
            # For streaming mode, extract from configs
            supplier_name = f"{supplier_config.get('supplier', 'Unknown')}_{brand_config.get('currency', 'Unknown')}_{month_abbr}_{year}_{brand_config.get('location', 'Unknown')}"
            brand = brand_config.get('brand', 'Unknown')
            location = brand_config.get('location', 'Unknown')
            currency = brand_config.get('currency', 'Unknown')
        
        # Process part numbers
        part_number = self._process_part_number(
            item.part_number,
            brand_config.get('minimumPartLength', 10),
            brand_config.get('partNumberSplice')
        )
        
        former_pn = self._process_part_number(
            item.former_part_number,
            brand_config.get('minimumPartLength', 10),
            brand_config.get('partNumberSplice')
        ) if item.former_part_number else ''
        
        supersede_pn = self._process_part_number(
            item.supersede_part_number,
            brand_config.get('minimumPartLength', 10),
            brand_config.get('partNumberSplice')
        ) if item.supersede_part_number else ''
        
        # Process price
        price = self._process_price(
            item.price,
            supplier_config.get('discount_percent', 0)
        )
        
        return {
            'Brand': brand,
            'Supplier Name': supplier_name,
            'Location': location,
            'Currency': currency,
            'Part Number': part_number,
            'Description': item.description or '',
            'FORMER PN': former_pn,
            'SUPERSESSION': supersede_pn,
            'Price': price
        }
    
    def _process_part_number(
        self,
        part_number: Optional[str],
        minimum_length: int,
        splice_position: Optional[int] = None
    ) -> str:
        """
        Process part number: remove special chars, pad with zeros, uppercase.
        
        Args:
            part_number: Raw part number
            minimum_length: Minimum length for padding
            splice_position: Position to splice from (if configured)
            
        Returns:
            Processed part number
        """
        if not part_number:
            return ''
        
        # Convert to string
        pn = str(part_number)
        
        # Apply splice if configured
        if splice_position is not None:
            pn = pn[splice_position:]
        
        # Remove special characters and spaces
        pn = re.sub(r'[^a-z0-9]', '', pn, flags=re.IGNORECASE)
        
        # Pad with zeros to minimum length
        pn = pn.zfill(minimum_length)
        
        # Convert to uppercase
        pn = pn.upper()
        
        return pn
    
    def _process_price(
        self,
        price: float,
        discount_percent: float
    ) -> float:
        """
        Process price: apply discount.
        
        Note: GST removal should be done during parsing if GST column exists.
        
        Args:
            price: Original price
            discount_percent: Discount percentage to apply
            
        Returns:
            Processed price (2 decimal places)
        """
        if price == 0:
            return 0.00
        
        try:
            # Convert to Decimal for precise calculation
            price_decimal = Decimal(str(price))
            
            # Apply discount if configured
            if discount_percent and discount_percent > 0:
                discount_decimal = Decimal(str(discount_percent))
                price_decimal = price_decimal - (price_decimal * discount_decimal / 100)
            
            # Round to 2 decimal places
            result = float(price_decimal.quantize(Decimal('0.01')))
            
            return result
            
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Price processing error: {str(e)}", price=price)
            return 0.00
