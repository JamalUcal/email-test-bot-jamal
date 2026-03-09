"""
Pricing data storage.

Stores parsed pricing data to Google Cloud Storage.
"""

import json
from datetime import datetime
from typing import List
from google.cloud import storage

from parsers.price_list_parser import ParsedPriceList, PriceListItem
from utils.logger import get_logger
from utils.exceptions import StorageError

logger = get_logger(__name__)


class PricingStorage:
    """Stores pricing data in GCS."""
    
    def __init__(self, bucket_name: str):
        """
        Initialize pricing storage.
        
        Args:
            bucket_name: GCS bucket name
        """
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        
        logger.info(f"PricingStorage initialized", bucket=bucket_name)
    
    def save_parsed_price_list(
        self,
        parsed_list: ParsedPriceList,
        email_id: str,
        email_date: datetime
    ) -> str:
        """
        Save parsed price list to GCS.
        
        Args:
            parsed_list: Parsed price list data
            email_id: Gmail message ID
            email_date: Email date
            
        Returns:
            GCS path where data was saved
            
        Raises:
            StorageError: If save fails
        """
        try:
            # Create path: pricing_data/{supplier}/{brand}/{YYYY-MM-DD}/{email_id}_{filename}.json
            date_str = email_date.strftime('%Y-%m-%d')
            path = f"pricing_data/{parsed_list.supplier}/{parsed_list.brand}/{date_str}/{email_id}_{parsed_list.filename}.json"
            
            # Convert to JSON-serializable format
            data = {
                'metadata': {
                    'supplier': parsed_list.supplier,
                    'brand': parsed_list.brand,
                    'location': parsed_list.location,
                    'currency': parsed_list.currency,
                    'filename': parsed_list.filename,
                    'email_id': email_id,
                    'email_date': email_date.isoformat(),
                    'processed_date': datetime.utcnow().isoformat(),
                    'total_rows': parsed_list.total_rows,
                    'valid_rows': parsed_list.valid_rows,
                    'errors': parsed_list.errors
                },
                'items': [
                    {
                        'part_number': item.part_number,
                        'description': item.description,
                        'price': item.price,
                        'former_part_number': item.former_part_number,
                        'supersede_part_number': item.supersede_part_number,
                        'brand': item.brand,
                        'location': item.location,
                        'currency': item.currency,
                        'row_number': item.row_number
                    }
                    for item in parsed_list.items
                ]
            }
            
            # Upload to GCS
            blob = self.bucket.blob(path)
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type='application/json'
            )
            
            logger.info(
                f"Saved price list data to GCS",
                path=path,
                supplier=parsed_list.supplier,
                brand=parsed_list.brand,
                items=len(parsed_list.items)
            )
            
            return f"gs://{self.bucket_name}/{path}"
            
        except Exception as e:
            error_msg = f"Failed to save price list: {str(e)}"
            logger.error(error_msg, error=str(e))
            raise StorageError(error_msg)
    
    def save_processing_summary(
        self,
        email_id: str,
        email_date: datetime,
        supplier: str,
        attachments_processed: int,
        items_extracted: int,
        errors: List[str]
    ) -> str:
        """
        Save processing summary for an email.
        
        Args:
            email_id: Gmail message ID
            email_date: Email date
            supplier: Supplier name
            attachments_processed: Number of attachments processed
            items_extracted: Total items extracted
            errors: List of errors encountered
            
        Returns:
            GCS path where summary was saved
        """
        try:
            date_str = email_date.strftime('%Y-%m-%d')
            path = f"processing_summaries/{supplier}/{date_str}/{email_id}_summary.json"
            
            summary = {
                'email_id': email_id,
                'email_date': email_date.isoformat(),
                'processed_date': datetime.utcnow().isoformat(),
                'supplier': supplier,
                'attachments_processed': attachments_processed,
                'items_extracted': items_extracted,
                'errors': errors
            }
            
            blob = self.bucket.blob(path)
            blob.upload_from_string(
                json.dumps(summary, indent=2),
                content_type='application/json'
            )
            
            logger.info(
                f"Saved processing summary",
                path=path,
                supplier=supplier,
                items=items_extracted
            )
            
            return f"gs://{self.bucket_name}/{path}"
            
        except Exception as e:
            logger.error(f"Failed to save summary: {str(e)}", error=str(e))
            # Don't raise - summary is not critical
            return ""
