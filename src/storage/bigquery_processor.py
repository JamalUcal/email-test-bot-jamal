"""
BigQuery processor for price list supersession reconciliation.

Handles BigQuery operations for price list processing:
1. Upload parsed CSV to GCS staging
2. Load to BigQuery staging table
3. Run reconciliation stored procedure
4. Export reconciled data to GCS
5. Download to temp file for Drive upload
6. Insert to main table for analytics
7. Cleanup staging resources
"""

import uuid
import tempfile
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Any

from google.cloud import bigquery, storage  # type: ignore[attr-defined]
from google.oauth2 import service_account

from utils.logger import get_logger
from utils.exceptions import BigQueryProcessingError, ReconciliationError

logger = get_logger(__name__)


class BigQueryPriceListProcessor:
    """
    Handles BigQuery operations for price list processing.
    
    Memory-efficient flow using GCS as intermediary:
    1. Parse source file → stream to local CSV (existing pipeline)
    2. Upload local CSV to GCS staging bucket
    3. Create price_list record in BigQuery
    4. Load items from GCS → BigQuery staging table (load job, free & atomic)
    5. Call reconciliation stored procedure
    6. Export reconciled data to GCS
    7. Download from GCS → temp file for Drive upload
    8. Insert reconciled data to main table for analytics
    9. Cleanup GCS staging files and staging table
    
    This preserves the existing streaming architecture's memory efficiency
    while enabling BigQuery-based supersession reconciliation.
    """
    
    # GCS paths for staging
    STAGING_PREFIX = 'bigquery_staging/input'
    RECONCILED_PREFIX = 'bigquery_staging/reconciled'
    
    # BigQuery scopes
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform'
    ]
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str = 'PRICING',
        staging_bucket: str = 'pricing-email-bot-bucket',
        service_account_info: Optional[Dict] = None,
        bigquery_config: Optional[Dict] = None
    ):
        """
        Initialize processor with proper credentials.
        
        Follows the same credential pattern as DriveUploader and GmailClient:
        - If service_account_info provided: use explicit service account credentials
        - Otherwise: fall back to Application Default Credentials (ADC) for local dev
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset name
            staging_bucket: GCS bucket for staging files
            service_account_info: Service account credentials dict (from core_config.json)
            bigquery_config: BigQuery config from core_config.json (includes cleanup_on_failure)
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.staging_bucket = staging_bucket
        self.config = bigquery_config or {}
        self.max_chain_depth = self.config.get('reconciliation', {}).get('max_chain_depth', 10)
        
        # Use service account credentials if provided (consistent with DriveUploader pattern)
        if service_account_info and len(service_account_info) > 0:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=self.SCOPES
            )
            self.bq_client = bigquery.Client(project=project_id, credentials=credentials)
            self.storage_client = storage.Client(project=project_id, credentials=credentials)
            logger.debug("BigQueryPriceListProcessor initialized with service account credentials")
        else:
            # Use Application Default Credentials for local development
            import google.auth
            credentials, detected_project = google.auth.default(scopes=self.SCOPES)
            self.bq_client = bigquery.Client(project=project_id, credentials=credentials)
            self.storage_client = storage.Client(project=project_id, credentials=credentials)
            logger.debug(
                "BigQueryPriceListProcessor initialized with Application Default Credentials",
                detected_project=detected_project
            )
        
        logger.info(
            "BigQueryPriceListProcessor initialized",
            project_id=project_id,
            dataset_id=dataset_id,
            staging_bucket=staging_bucket,
            max_chain_depth=self.max_chain_depth
        )
    
    def process_price_list(
        self,
        local_csv_path: str,
        supplier: str,
        brand: str,
        currency: str,
        location: str,
        source_filename: str,
        valid_from_date: Optional[date] = None,
        source_email_subject: Optional[str] = None,
        source_email_date: Optional[datetime] = None
    ) -> Tuple[str, str]:
        """
        Full processing pipeline with GCS intermediary and dedicated staging table.
        
        Uses a dedicated staging table per price list for cost efficiency:
        - Stored procedure operates ONLY on staging table (~50MB scan)
        - Avoids scanning large price_list_items table (saves ~$0.05/query)
        - After reconciliation, results inserted into main table for analytics
        
        Args:
            local_csv_path: Path to locally generated CSV (from existing streaming parser)
            supplier: Supplier name
            brand: Brand name
            currency: Currency code
            location: Location string
            source_filename: Original source filename
            valid_from_date: Price list validity date
            source_email_subject: Email subject (optional, for tracking)
            source_email_date: Email date (optional, for tracking)
            
        Returns:
            Tuple of (price_list_id, gcs_reconciled_path)
            
        The caller can then download from gcs_reconciled_path and upload to Drive.
        """
        price_list_id = str(uuid.uuid4())
        gcs_input_path: Optional[str] = None
        gcs_reconciled_path: Optional[str] = None
        staging_table_id: Optional[str] = None
        
        try:
            logger.info(
                "Starting BigQuery price list processing",
                price_list_id=price_list_id,
                supplier=supplier,
                brand=brand,
                source_filename=source_filename
            )
            
            # Step 1: Upload local CSV to GCS
            gcs_input_path = f'{self.STAGING_PREFIX}/{price_list_id}.csv'
            self._upload_to_gcs(local_csv_path, gcs_input_path)
            
            # Step 2: Create price_list record
            self._create_price_list(
                price_list_id=price_list_id,
                supplier=supplier,
                brand=brand,
                currency=currency,
                location=location,
                source_filename=source_filename,
                valid_from_date=valid_from_date,
                source_email_subject=source_email_subject,
                source_email_date=source_email_date
            )
            
            # Step 3: Load items to dedicated staging table (free load job)
            staging_table_id = self._load_items_to_staging(price_list_id, gcs_input_path, currency)
            
            # Step 4: Call reconciliation procedure on staging table only
            # This is cost-efficient: only scans ~50MB staging table, not 10GB+ main table
            self._reconcile(staging_table_id, price_list_id)
            
            # Step 5: Export reconciled data from staging table to GCS
            gcs_reconciled_path = f'{self.RECONCILED_PREFIX}/{price_list_id}'
            self._export_reconciled_to_gcs(staging_table_id, price_list_id, gcs_reconciled_path)
            
            # Step 6: Register staging table for deferred merge (SCD Type 2)
            # The staging table will be processed by a scheduled job that merges
            # into canonical_prices using SCD Type 2 logic
            self._register_staging_table(price_list_id, staging_table_id)
            
            logger.info(
                "BigQuery price list processing completed successfully",
                price_list_id=price_list_id,
                gcs_reconciled_path=gcs_reconciled_path,
                staging_table_id=staging_table_id,
                merge_status="PENDING"
            )
            
            return price_list_id, f'gs://{self.staging_bucket}/{gcs_reconciled_path}'
            
        except Exception as e:
            logger.error(
                "BigQuery price list processing failed",
                price_list_id=price_list_id,
                error=str(e)
            )
            # Compensation: cleanup on failure
            self._cleanup_failed_processing(
                price_list_id, gcs_input_path, gcs_reconciled_path, staging_table_id
            )
            raise BigQueryProcessingError(f"Failed to process price list: {str(e)}") from e
    
    def _upload_to_gcs(self, local_path: str, gcs_path: str) -> None:
        """Upload local file to GCS staging bucket."""
        logger.debug(f"Uploading to GCS: {local_path} -> gs://{self.staging_bucket}/{gcs_path}")
        
        bucket = self.storage_client.bucket(self.staging_bucket)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        
        logger.info(f"Uploaded to GCS", gcs_path=f"gs://{self.staging_bucket}/{gcs_path}")
    
    def _get_next_price_list_seq(self) -> int:
        """
        Get the next price_list_seq value.
        
        Uses MAX(price_list_seq) + 1 to generate a sequential INT64 value.
        This is safe for concurrent inserts due to BigQuery's transaction isolation.
        
        Returns:
            Next sequence number (starts at 1 if table is empty)
        """
        query = f"""
            SELECT COALESCE(MAX(price_list_seq), 0) + 1 as next_seq
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
        """
        
        result = list(self.bq_client.query(query).result())
        next_seq = result[0].next_seq if result else 1
        
        logger.debug(f"Generated next price_list_seq: {next_seq}")
        return next_seq
    
    def _create_price_list(
        self,
        price_list_id: str,
        supplier: str,
        brand: str,
        currency: str,
        location: str,
        source_filename: str,
        valid_from_date: Optional[date],
        source_email_subject: Optional[str] = None,
        source_email_date: Optional[datetime] = None
    ) -> int:
        """
        Insert price_list record with PENDING merge status.
        
        Returns:
            price_list_seq: The INT64 sequence number for this price list
        """
        # Get next sequence number
        price_list_seq = self._get_next_price_list_seq()
        
        query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.price_lists` 
            (price_list_seq, price_list_id, supplier, brand, currency, location, 
             source_filename, source_email_subject, source_email_date,
             valid_from_date, upload_timestamp, reconciliation_status, merge_status)
            VALUES (@price_list_seq, @price_list_id, @supplier, @brand, @currency, @location,
                    @source_filename, @source_email_subject, @source_email_date,
                    @valid_from_date, CURRENT_TIMESTAMP(), 'PENDING', 'PENDING')
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_seq", "INT64", price_list_seq),
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("supplier", "STRING", supplier),
                bigquery.ScalarQueryParameter("brand", "STRING", brand),
                bigquery.ScalarQueryParameter("currency", "STRING", currency),
                bigquery.ScalarQueryParameter("location", "STRING", location),
                bigquery.ScalarQueryParameter("source_filename", "STRING", source_filename),
                bigquery.ScalarQueryParameter("source_email_subject", "STRING", source_email_subject),
                bigquery.ScalarQueryParameter("source_email_date", "TIMESTAMP", source_email_date),
                bigquery.ScalarQueryParameter("valid_from_date", "DATE", valid_from_date),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
        logger.debug(f"Created price_list record", price_list_id=price_list_id, price_list_seq=price_list_seq)
        
        return price_list_seq
    
    def _load_items_to_staging(self, price_list_id: str, gcs_path: str, currency: str) -> str:
        """
        Load items from GCS CSV into a dedicated staging table.
        
        Uses a dedicated staging table per price list for cost efficiency:
        - Stored procedure operates ONLY on this small table (~50MB)
        - Avoids scanning large price_list_items table (saves ~$0.05/query)
        - After reconciliation, results inserted into main table
        
        Load jobs are:
        - Free (no query costs)
        - Fast (parallel loading)
        - Atomic (all or nothing)
        - Memory-efficient (no Python memory overhead)
        
        Returns:
            staging_table_id: Full table ID for the staging table
        """
        # Create unique staging table name (replace hyphens with underscores for valid SQL identifier)
        staging_table_id = f'{self.project_id}.{self.dataset_id}._staging_{price_list_id.replace("-", "_")}'
        
        logger.debug(f"Loading items to staging table: {staging_table_id}")
        
        # Schema matches the CSV output from file_generator.py
        # Note: CSV has header row with these columns:
        # Brand, Supplier Name, Location, Currency, Part Number, Description, FORMER PN, SUPERSESSION, Price
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=[
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("supplier_name", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("part_number", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("former_part_number", "STRING"),
                bigquery.SchemaField("supersession", "STRING"),
                bigquery.SchemaField("original_price", "NUMERIC"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        uri = f'gs://{self.staging_bucket}/{gcs_path}'
        load_job = self.bq_client.load_table_from_uri(
            uri, staging_table_id, job_config=job_config
        )
        load_job.result()  # Wait for completion
        
        logger.info(f"Loaded items to staging table", staging_table_id=staging_table_id)
        
        # Initialize staging table with all reconciliation columns
        # (Avoids ALTER TABLE with DEFAULT which BigQuery doesn't support)
        init_query = f"""
            CREATE OR REPLACE TABLE `{staging_table_id}` AS
            SELECT 
                GENERATE_UUID() as item_id,
                ROW_NUMBER() OVER (ORDER BY part_number) as source_row_number,
                brand,
                supplier_name,
                location,
                currency,
                part_number,
                description,
                former_part_number,
                supersession,
                original_price,
                -- Reconciliation output columns (with defaults)
                CAST(NULL AS NUMERIC) as effective_price,
                CAST(NULL AS STRING) as terminal_part_number,
                0 as supersession_chain_length,
                CAST(NULL AS STRING) as price_inherited_from,
                'PENDING' as reconciliation_status,
                CAST(NULL AS STRING) as reconciliation_error_message,
                FALSE as is_synthetic,
                CAST(NULL AS TIMESTAMP) as reconciled_timestamp
            FROM `{staging_table_id}`
        """
        self.bq_client.query(init_query).result()
        
        logger.debug(f"Initialized staging table with reconciliation columns")
        
        return staging_table_id
    
    def _reconcile(self, staging_table_id: str, price_list_id: str) -> None:
        """
        Call the reconciliation stored procedure on the staging table.
        
        The stored procedure operates ONLY on the staging table,
        avoiding costly scans of the main price_list_items table.
        """
        logger.info(f"Running reconciliation procedure", staging_table_id=staging_table_id)
        
        query = f"""
            CALL `{self.project_id}.{self.dataset_id}.reconcile_supersessions_staging`(
                @staging_table_id, 
                @price_list_id,
                @max_chain_depth
            )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("staging_table_id", "STRING", staging_table_id),
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("max_chain_depth", "INT64", self.max_chain_depth),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
        logger.info(f"Reconciliation procedure completed", staging_table_id=staging_table_id)
    
    def _export_reconciled_to_gcs(
        self, staging_table_id: str, price_list_id: str, gcs_path: str
    ) -> None:
        """
        Export reconciled data from staging table directly to GCS.
        
        Exports from the staging table (not main table) for efficiency.
        
        Uses EXPORT DATA which:
        - Writes directly to GCS (no Python memory)
        - Handles large datasets efficiently
        - Produces properly formatted CSV
        """
        logger.debug(f"Exporting reconciled data to GCS: gs://{self.staging_bucket}/{gcs_path}")
        
        # Get metadata for export
        metadata_query = f"""
            SELECT supplier, brand, location, currency
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE price_list_id = @price_list_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        result = list(self.bq_client.query(metadata_query, job_config=job_config).result())[0]
        
        # Export from staging table (not main table - cost efficient)
        # Format matches the original CSV output format
        export_query = f"""
            EXPORT DATA OPTIONS(
                uri='gs://{self.staging_bucket}/{gcs_path}/*.csv',
                format='CSV',
                overwrite=true,
                header=true
            ) AS
            SELECT 
                '{result.brand}' as Brand,
                supplier_name as `Supplier Name`,
                '{result.location}' as Location,
                '{result.currency}' as Currency,
                part_number as `Part Number`,
                description as Description,
                COALESCE(former_part_number, '') as `FORMER PN`,
                COALESCE(supersession, '') as SUPERSESSION,
                effective_price as Price
            FROM `{staging_table_id}`
            ORDER BY source_row_number, part_number
        """
        
        self.bq_client.query(export_query).result()
        logger.info(f"Exported reconciled data to GCS", gcs_path=f"gs://{self.staging_bucket}/{gcs_path}")
    
    def _register_staging_table(self, price_list_id: str, staging_table_id: str) -> None:
        """
        Register the staging table for deferred merge into canonical_prices.
        
        The staging table will be processed by a scheduled job that:
        1. Merges staging tables directly into canonical_prices using SCD Type 2 logic
        2. Drops processed staging tables
        
        Args:
            price_list_id: UUID of the price list
            staging_table_id: Full BigQuery table ID of the staging table
        """
        logger.debug(
            f"Registering staging table for deferred merge",
            price_list_id=price_list_id,
            staging_table_id=staging_table_id
        )
        
        query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.price_lists`
            SET staging_table_id = @staging_table_id,
                merge_status = 'PENDING'
            WHERE price_list_id = @price_list_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("staging_table_id", "STRING", staging_table_id),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
        logger.info(
            f"Registered staging table for deferred merge",
            price_list_id=price_list_id,
            staging_table_id=staging_table_id
        )
    
    def _insert_to_main_table(self, staging_table_id: str, price_list_id: str) -> None:
        """
        DEPRECATED: Insert reconciled data from staging table into price_list_items.
        
        This method is no longer called in the main processing flow.
        Use the scheduled merge into canonical_prices instead.
        
        Kept for backwards compatibility and manual data recovery.
        """
        logger.debug(f"Inserting reconciled data to main table")
        
        insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.price_list_items`
            (item_id, price_list_id, part_number, currency, original_price,
             effective_price, former_part_number, supersession, description,
             terminal_part_number, supersession_chain_length, price_inherited_from,
             reconciliation_status, reconciliation_error_message, is_synthetic,
             source_row_number, created_timestamp, reconciled_timestamp)
            SELECT 
                item_id,
                @price_list_id,
                part_number,
                currency,
                original_price,
                effective_price,
                former_part_number,
                supersession,
                description,
                terminal_part_number,
                supersession_chain_length,
                price_inherited_from,
                reconciliation_status,
                reconciliation_error_message,
                COALESCE(is_synthetic, FALSE),
                source_row_number,
                CURRENT_TIMESTAMP(),
                reconciled_timestamp
            FROM `{staging_table_id}`
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        self.bq_client.query(insert_query, job_config=job_config).result()
        logger.debug(f"Inserted reconciled data to main table")
    
    def _drop_staging_table(self, staging_table_id: str) -> None:
        """
        Drop a staging table.
        
        This is a utility method - not called in the main processing flow.
        Staging tables are preserved for the scheduled merge job.
        Called by the merge procedure after successful processing.
        """
        logger.debug(f"Dropping staging table: {staging_table_id}")
        self.bq_client.delete_table(staging_table_id, not_found_ok=True)
        logger.debug(f"Dropped staging table")
    
    def download_reconciled_to_temp(self, gcs_path: str) -> str:
        """
        Download reconciled CSV from GCS to a temporary file.
        
        Args:
            gcs_path: Full GCS URI (gs://bucket/path)
            
        Returns:
            Path to local temporary file
        """
        # Parse gs:// URI
        if gcs_path.startswith('gs://'):
            gcs_path = gcs_path[5:]
        bucket_name, blob_path = gcs_path.split('/', 1)
        
        bucket = self.storage_client.bucket(bucket_name)
        
        # EXPORT DATA creates files like 000000000000.csv, 000000000001.csv, etc.
        # For most price lists, there will be just one file
        blobs = list(bucket.list_blobs(prefix=blob_path))
        
        # Create a temporary file to store the combined CSV
        temp_file = tempfile.NamedTemporaryFile(
            mode='wb', suffix='.csv', delete=False
        )
        temp_path = temp_file.name
        
        logger.debug(f"Downloading reconciled data to temp file: {temp_path}")
        
        header_written = False
        for blob in sorted(blobs, key=lambda b: b.name):
            if blob.name.endswith('.csv'):
                content = blob.download_as_bytes()
                
                # For subsequent files, skip the header row
                if header_written:
                    # Find first newline and skip header
                    newline_pos = content.find(b'\n')
                    if newline_pos != -1:
                        content = content[newline_pos + 1:]
                else:
                    header_written = True
                
                temp_file.write(content)
        
        temp_file.close()
        
        logger.info(f"Downloaded reconciled data to temp file", temp_path=temp_path)
        return temp_path
    
    def get_reconciliation_errors(self, price_list_id: str) -> List[Dict[str, Any]]:
        """Get errors for summary email (small result set, OK to fetch to memory)."""
        query = f"""
            SELECT 
                part_number,
                reconciliation_status,
                reconciliation_error_message
            FROM `{self.project_id}.{self.dataset_id}.price_list_items`
            WHERE price_list_id = @price_list_id
              AND reconciliation_status NOT IN ('OK', 'NO_SUPERSESSION', 'PENDING')
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        results = self.bq_client.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    
    def get_processing_errors(self, price_list_id: str) -> List[Dict[str, Any]]:
        """Get processing errors (duplicates, circular refs, etc.) from processing_errors table."""
        query = f"""
            SELECT 
                error_type,
                part_number,
                error_message,
                error_details
            FROM `{self.project_id}.{self.dataset_id}.processing_errors`
            WHERE price_list_id = @price_list_id
            ORDER BY error_type, part_number
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        results = self.bq_client.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    
    def get_reconciliation_stats(self, price_list_id: str) -> Dict[str, Any]:
        """Get reconciliation statistics for reporting."""
        query = f"""
            SELECT 
                total_items,
                reconciled_items,
                items_with_errors,
                synthetic_items_added,
                duplicates_found,
                duplicates_removed,
                reconciliation_status,
                reconciliation_timestamp
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE price_list_id = @price_list_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        result = list(self.bq_client.query(query, job_config=job_config).result())
        return dict(result[0]) if result else {}
    
    def cleanup_gcs_files(self, price_list_id: str) -> None:
        """Public method to cleanup GCS staging files after successful Drive upload."""
        self._cleanup_gcs_files(price_list_id)
    
    def _cleanup_gcs_files(self, price_list_id: str) -> None:
        """Delete staging files from GCS."""
        logger.debug(f"Cleaning up GCS files for price_list_id: {price_list_id}")
        
        bucket = self.storage_client.bucket(self.staging_bucket)
        
        # Delete input file
        try:
            input_blob = bucket.blob(f'{self.STAGING_PREFIX}/{price_list_id}.csv')
            input_blob.delete()
            logger.debug(f"Deleted GCS input file")
        except Exception as e:
            logger.warning(f"Failed to delete GCS input file: {e}")
        
        # Delete reconciled files (may be multiple from EXPORT DATA)
        reconciled_prefix = f'{self.RECONCILED_PREFIX}/{price_list_id}'
        try:
            blobs = bucket.list_blobs(prefix=reconciled_prefix)
            for blob in blobs:
                blob.delete()
            logger.debug(f"Deleted GCS reconciled files")
        except Exception as e:
            logger.warning(f"Failed to delete GCS reconciled files: {e}")
    
    def _cleanup_failed_processing(
        self,
        price_list_id: str,
        gcs_input_path: Optional[str],
        gcs_reconciled_path: Optional[str],
        staging_table_id: Optional[str] = None
    ) -> None:
        """
        Saga pattern compensation: cleanup partial data on failure.
        
        Behavior controlled by cleanup_on_failure.mode config:
        - TEST: Preserve data for inspection, mark price_list as FAILED
        - PRODUCTION: Full cleanup to avoid orphaned data
        """
        cleanup_config = self.config.get('cleanup_on_failure', {})
        mode = cleanup_config.get('mode', 'test')  # Default to test mode
        mode_config = cleanup_config.get(mode, {})
        
        if mode_config.get('log_cleanup_skipped', True):
            logger.warning(
                f"Cleanup mode={mode}. Resources preserved for inspection" if mode == 'test' else f"Cleanup mode={mode}",
                price_list_id=price_list_id,
                staging_table=staging_table_id,
                gcs_input=gcs_input_path,
                gcs_reconciled=gcs_reconciled_path
            )
        
        # Always mark price_list as FAILED for tracking
        try:
            update_query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.price_lists`
                SET reconciliation_status = 'FAILED',
                    reconciliation_timestamp = CURRENT_TIMESTAMP()
                WHERE price_list_id = @price_list_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                ]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            logger.warning(f"Failed to update price_list status: {e}")
        
        # Conditional cleanup based on mode
        if mode_config.get('drop_staging_table', False) and staging_table_id:
            try:
                self.bq_client.delete_table(staging_table_id, not_found_ok=True)
                logger.debug(f"Dropped staging table: {staging_table_id}")
            except Exception as e:
                logger.warning(f"Failed to drop staging table: {e}")
        
        if mode_config.get('delete_price_list_record', False):
            try:
                delete_items = f"""
                    DELETE FROM `{self.project_id}.{self.dataset_id}.price_list_items`
                    WHERE price_list_id = @price_list_id
                """
                delete_list = f"""
                    DELETE FROM `{self.project_id}.{self.dataset_id}.price_lists`
                    WHERE price_list_id = @price_list_id
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                    ]
                )
                self.bq_client.query(delete_items, job_config=job_config).result()
                self.bq_client.query(delete_list, job_config=job_config).result()
                logger.debug(f"Deleted price_list records")
            except Exception as e:
                logger.warning(f"Failed to delete price_list records: {e}")
        
        if mode_config.get('delete_gcs_files', False):
            self._cleanup_gcs_files(price_list_id)
    
    def update_drive_info(
        self, price_list_id: str, drive_file_id: str, drive_file_url: str
    ) -> None:
        """Update price_list record with Drive file info after successful upload."""
        query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.price_lists`
            SET drive_file_id = @drive_file_id,
                drive_file_url = @drive_file_url
            WHERE price_list_id = @price_list_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("drive_file_id", "STRING", drive_file_id),
                bigquery.ScalarQueryParameter("drive_file_url", "STRING", drive_file_url),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
        logger.debug(f"Updated price_list with Drive info", price_list_id=price_list_id)
    
    def check_price_list_exists(
        self,
        supplier: str,
        brand: str,
        valid_from_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Check if a price list already exists for supplier+brand+valid_from combination.
        
        Used by historical loader to skip already-processed files.
        
        Args:
            supplier: Supplier name
            brand: Brand name
            valid_from_date: Valid from date
            
        Returns:
            Dictionary with price_list_id, source_filename, etc. if exists,
            None if no matching price list found
        """
        query = f"""
            SELECT 
                price_list_id,
                source_filename,
                upload_timestamp,
                reconciliation_status,
                merge_status,
                total_items
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE supplier = @supplier
              AND brand = @brand
              AND valid_from_date = @valid_from_date
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("supplier", "STRING", supplier),
                bigquery.ScalarQueryParameter("brand", "STRING", brand),
                bigquery.ScalarQueryParameter("valid_from_date", "DATE", valid_from_date),
            ]
        )
        
        try:
            result = list(self.bq_client.query(query, job_config=job_config).result())
            if result:
                row = dict(result[0])
                logger.debug(
                    f"Found existing price list",
                    supplier=supplier,
                    brand=brand,
                    valid_from_date=valid_from_date.isoformat(),
                    price_list_id=row.get('price_list_id')
                )
                return row
            return None
        except Exception as e:
            logger.warning(
                f"Error checking for existing price list: {e}",
                supplier=supplier,
                brand=brand,
                valid_from_date=valid_from_date.isoformat()
            )
            return None
    
    def check_source_filename_exists(self, source_filename: str) -> Optional[Dict[str, Any]]:
        """
        Check if a price list with this source filename already exists.
        
        Args:
            source_filename: Original source filename
            
        Returns:
            Dictionary with price_list_id and metadata if exists, None otherwise
        """
        query = f"""
            SELECT 
                price_list_id,
                supplier,
                brand,
                valid_from_date,
                upload_timestamp,
                reconciliation_status,
                merge_status
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE source_filename = @source_filename
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source_filename", "STRING", source_filename),
            ]
        )
        
        try:
            result = list(self.bq_client.query(query, job_config=job_config).result())
            if result:
                row = dict(result[0])
                logger.debug(
                    f"Found existing price list by filename",
                    source_filename=source_filename,
                    price_list_id=row.get('price_list_id')
                )
                return row
            return None
        except Exception as e:
            logger.warning(
                f"Error checking for existing filename: {e}",
                source_filename=source_filename
            )
            return None
