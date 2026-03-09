"""
State management using Google Cloud Storage or local file.

Manages the last processed timestamp to avoid reprocessing emails.
Supports both GCS (production) and local file (development).
"""

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any
from pathlib import Path

from utils.logger import setup_logger
from utils.exceptions import StateManagementError

logger = setup_logger(__name__)


class StateManager:
    """Manages state persistence in GCS or local file."""
    
    def __init__(
        self,
        bucket_name: str,
        state_file_path: str,
        use_local: bool = False,
        local_path: Optional[str] = None
    ):
        """
        Initialize state manager.
        
        Args:
            bucket_name: GCS bucket name (ignored if use_local=True)
            state_file_path: Path to state file within bucket or local filename
            use_local: If True, use local file instead of GCS
            local_path: Local directory for state file (default: ./state/)
        """
        self.bucket_name = bucket_name
        self.state_file_path = state_file_path
        self.use_local = use_local
        
        if use_local:
            # Local file mode for development
            self.local_dir = Path(local_path or "./state")
            self.local_dir.mkdir(parents=True, exist_ok=True)
            self.local_file = self.local_dir / Path(state_file_path).name
            
            logger.debug(
                "StateManager initialized (LOCAL MODE)",
                local_file=str(self.local_file)
            )
        else:
            # GCS mode for production
            from google.cloud import storage
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(bucket_name)
            self.blob = self.bucket.blob(state_file_path)
            
            logger.debug(
                "StateManager initialized (GCS MODE)",
                bucket=bucket_name,
                state_file=state_file_path
            )
    
    def get_state(self) -> Dict[str, Any]:
        """
        Retrieve current state from GCS or local file.
        
        Returns:
            Dictionary containing state data
            
        Raises:
            StateManagementError: If state cannot be retrieved
        """
        try:
            if self.use_local:
                # Local file mode
                if not self.local_file.exists():
                    logger.warning("Local state file does not exist, returning default state")
                    return self._get_default_state()
                
                with open(self.local_file, 'r') as f:
                    state: Dict[str, Any] = json.load(f)
            else:
                # GCS mode
                if not self.blob.exists():
                    logger.warning("GCS state file does not exist, returning default state")
                    return self._get_default_state()
                
                state_json = self.blob.download_as_text()
                state = json.loads(state_json)
            
            logger.debug(
                "State retrieved successfully",
                last_processed=state.get('last_processed_timestamp'),
                last_execution=state.get('last_execution_timestamp'),
                mode="local" if self.use_local else "gcs"
            )
            
            return state
            
        except Exception as e:
            logger.error(f"Failed to retrieve state: {str(e)}")
            raise StateManagementError(f"Failed to retrieve state: {str(e)}")
    
    def update_state(self, state: Dict[str, Any]) -> None:
        """
        Update state in GCS or local file.
        
        Args:
            state: Dictionary containing state data
            
        Raises:
            StateManagementError: If state cannot be updated
        """
        try:
            # Ensure all required keys exist (prevent race condition corruption)
            if 'suppliers' not in state:
                state['suppliers'] = {}
            if 'last_scraped' not in state:
                state['last_scraped'] = {}
            if 'pending_results' not in state:
                state['pending_results'] = []
            if 'last_summary_sent_timestamp' not in state:
                state['last_summary_sent_timestamp'] = None
            
            state['version'] = '1.0.0'
            state_json = json.dumps(state, indent=2)
            
            if self.use_local:
                # Local file mode
                with open(self.local_file, 'w') as f:
                    f.write(state_json)
            else:
                # GCS mode
                self.blob.upload_from_string(
                    state_json,
                    content_type='application/json'
                )
            
            logger.info("State updated successfully", mode="local" if self.use_local else "gcs")
            
        except Exception as e:
            logger.error(f"Failed to update state: {str(e)}")
            raise StateManagementError(f"Failed to update state: {str(e)}")
    
    def get_last_processed_timestamp(self) -> Optional[str]:
        """
        Get the last processed email timestamp.
        
        Returns:
            ISO 8601 timestamp string or None if not set
        """
        state = self.get_state()
        return state.get('last_processed_timestamp')
    
    def update_last_processed(self, timestamp: str) -> None:
        """
        Update the last processed email timestamp.
        
        Args:
            timestamp: ISO 8601 timestamp string
        """
        state = self.get_state()
        state['last_processed_timestamp'] = timestamp
        self.update_state(state)
        
        logger.info(f"Updated last processed timestamp", timestamp=timestamp)
    
    def get_last_execution_timestamp(self) -> Optional[str]:
        """
        Get the last execution timestamp.
        
        Returns:
            ISO 8601 timestamp string or None if not set
        """
        state = self.get_state()
        return state.get('last_execution_timestamp')
    
    def update_last_execution(self) -> None:
        """Update the last execution timestamp to now."""
        now = datetime.now(timezone.utc).isoformat()
        state = self.get_state()
        state['last_execution_timestamp'] = now
        self.update_state(state)
        
        logger.info(f"Updated last execution timestamp", timestamp=now)
    
    def get_last_scraped_timestamp(self, supplier: str) -> Optional[str]:
        """
        Get the last scraped timestamp for a specific supplier.
        
        Args:
            supplier: Supplier name
            
        Returns:
            ISO 8601 timestamp string or None if not set
        """
        state = self.get_state()
        last_scraped: Dict[str, str] = state.get('last_scraped', {})
        return last_scraped.get(supplier)
    
    def update_last_scraped(self, supplier: str, timestamp: str) -> None:
        """
        Update the last scraped timestamp for a specific supplier.
        
        Args:
            supplier: Supplier name
            timestamp: ISO 8601 timestamp string
        """
        state = self.get_state()
        if 'last_scraped' not in state:
            state['last_scraped'] = {}
        
        state['last_scraped'][supplier] = timestamp
        self.update_state(state)
        
        logger.info(f"Updated last scraped timestamp for {supplier}", timestamp=timestamp)
    
    def get_all_last_scraped(self) -> Dict[str, str]:
        """
        Get all last scraped timestamps.
        
        Returns:
            Dictionary mapping supplier names to timestamps
        """
        state = self.get_state()
        last_scraped: Dict[str, str] = state.get('last_scraped', {})
        return last_scraped
    
    def get_supplier_state(self, supplier: str) -> Dict[str, Any]:
        """
        Get complete state for a supplier including downloaded files.
        
        Args:
            supplier: Supplier name
            
        Returns:
            Supplier state dictionary with keys:
            - last_run: Last execution timestamp (ISO 8601)
            - last_version: Month/date identifier for incremental detection
            - downloaded_files: List of dicts with {supplier_filename, valid_from_date, drive_file_id, timestamp}
            - interrupted: Whether last run was interrupted
            - last_file_index: Index for resuming interrupted downloads
        """
        state = self.get_state()
        suppliers_state: Dict[str, Dict[str, Any]] = state.get('suppliers', {})
        
        return suppliers_state.get(supplier, {
            'last_run': None,
            'last_version': None,
            'downloaded_files': [],
            'interrupted': False,
            'last_file_index': 0
        })
    
    def update_supplier_state(self, supplier: str, supplier_state: Dict[str, Any]) -> None:
        """
        Update complete state for a supplier.
        
        Args:
            supplier: Supplier name
            supplier_state: Complete supplier state dictionary
        """
        state = self.get_state()
        if 'suppliers' not in state:
            state['suppliers'] = {}
        
        # Update timestamp
        supplier_state['last_run'] = datetime.now(timezone.utc).isoformat()
        state['suppliers'][supplier] = supplier_state
        
        self.update_state(state)
        
        logger.info(
            f"Updated supplier state for {supplier}",
            last_version=supplier_state.get('last_version'),
            file_count=len(supplier_state.get('downloaded_files', [])),
            interrupted=supplier_state.get('interrupted', False)
        )
    
    def mark_supplier_interrupted(self, supplier: str, last_file_index: int) -> None:
        """
        Mark supplier as interrupted for resume.
        
        Args:
            supplier: Supplier name
            last_file_index: Index of last successfully processed file
        """
        supplier_state = self.get_supplier_state(supplier)
        supplier_state['interrupted'] = True
        supplier_state['last_file_index'] = last_file_index
        self.update_supplier_state(supplier, supplier_state)
        
        logger.warning(
            f"Marked {supplier} as interrupted",
            last_file_index=last_file_index
        )
    
    def clear_supplier_interrupted(self, supplier: str) -> None:
        """
        Clear interrupted flag after successful completion.
        
        Args:
            supplier: Supplier name
        """
        supplier_state = self.get_supplier_state(supplier)
        supplier_state['interrupted'] = False
        supplier_state['last_file_index'] = 0
        self.update_supplier_state(supplier, supplier_state)
        
        logger.info(f"Cleared interrupted flag for {supplier}")
    
    def update_file_progress(self, supplier: str, file_index: int, total_files: int) -> None:
        """
        Update progress after successfully uploading a file.
        
        Args:
            supplier: Supplier name
            file_index: Index of file just processed (1-based)
            total_files: Total number of files being processed
        """
        supplier_state = self.get_supplier_state(supplier)
        supplier_state['last_file_index'] = file_index
        supplier_state['files_processed'] = file_index
        supplier_state['total_files'] = total_files
        supplier_state['last_progress_update'] = datetime.now(timezone.utc).isoformat()
        self.update_supplier_state(supplier, supplier_state)
        
        logger.info(
            f"Updated progress for {supplier}",
            files_processed=file_index,
            total_files=total_files,
            progress_percentage=round((file_index / total_files) * 100, 1) if total_files > 0 else 0
        )
    
    def is_file_already_processed(
        self,
        supplier: str,
        supplier_filename: str,
        valid_from_date: Optional[str] = None
    ) -> bool:
        """
        Check if a file has already been processed.
        
        Matches by supplier_filename + valid_from_date for accurate duplicate detection.
        
        Args:
            supplier: Supplier name
            supplier_filename: Supplier's original filename (required)
            valid_from_date: Valid from date (ISO format string, optional)
            
        Returns:
            True if file was already processed
        """
        supplier_state = self.get_supplier_state(supplier)
        downloaded_files = supplier_state.get('downloaded_files', [])
        
        logger.debug(
            f"[STATE CHECK] Checking if already processed: supplier={supplier}, supplier_filename={supplier_filename}, valid_from={valid_from_date}",
            supplier=supplier,
            supplier_filename=supplier_filename,
            valid_from_date=valid_from_date,
            state_file_count=len(downloaded_files)
        )
        
        # Check if we have a matching file
        for file_record in downloaded_files:
            # Match by supplier_filename and valid_from date
            if file_record.get('supplier_filename') == supplier_filename:
                # If both have valid_from_date, they must match
                if valid_from_date and file_record.get('valid_from_date'):
                    if file_record.get('valid_from_date') == valid_from_date:
                        logger.debug(
                            f"✓ [DUPLICATE FOUND] File already processed: {supplier} - {supplier_filename} - {valid_from_date}",
                            supplier=supplier,
                            supplier_filename=supplier_filename,
                            valid_from_date=valid_from_date
                        )
                        return True
                # If checking file has date but stored doesn't, match by supplier_filename only
                elif valid_from_date and not file_record.get('valid_from_date'):
                    logger.debug(
                        f"✓ [DUPLICATE FOUND] File already processed (by supplier_filename, stored has no date): {supplier} - {supplier_filename}",
                        supplier=supplier,
                        supplier_filename=supplier_filename,
                        checking_with_date=valid_from_date,
                        stored_date=None
                    )
                    return True
                # If neither has date, match by supplier_filename only
                elif not valid_from_date and not file_record.get('valid_from_date'):
                    logger.debug(
                        f"✓ [DUPLICATE FOUND] File already processed (by supplier_filename, no dates): {supplier} - {supplier_filename}",
                        supplier=supplier,
                        supplier_filename=supplier_filename
                    )
                    return True
        
        logger.debug(
            f"✗ [NOT IN STATE] File not previously processed: {supplier} - {supplier_filename} - {valid_from_date}",
            supplier=supplier,
            supplier_filename=supplier_filename,
            valid_from_date=valid_from_date
        )
        return False
    
    def add_downloaded_file(
        self,
        supplier: str,
        supplier_filename: str,
        valid_from_date: Optional[str] = None,
        drive_file_id: Optional[str] = None
    ) -> None:
        """
        Add a downloaded file to supplier state.
        
        Args:
            supplier: Supplier name
            supplier_filename: Supplier's original filename (used for duplicate detection)
            valid_from_date: Valid from date (ISO format string, used for duplicate detection)
            drive_file_id: Google Drive file ID (for audit purposes)
        """
        supplier_state = self.get_supplier_state(supplier)
        
        file_record = {
            'supplier_filename': supplier_filename,
            'valid_from_date': valid_from_date,
            'drive_file_id': drive_file_id,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        supplier_state['downloaded_files'].append(file_record)
        self.update_supplier_state(supplier, supplier_state)
        
        logger.info(
            f"Added downloaded file for {supplier}",
            supplier_filename=supplier_filename,
            valid_from_date=valid_from_date,
            drive_file_id=drive_file_id
        )
    
    def cleanup_old_files(self, supplier: str, retention_days: int = 90) -> int:
        """
        Remove file entries older than retention_days from supplier state.
        
        This prevents the state file from growing indefinitely by purging
        old file records that are no longer needed for duplicate detection.
        
        Args:
            supplier: Supplier name
            retention_days: Number of days to retain file records (default: 90)
            
        Returns:
            Number of entries removed
        """
        supplier_state = self.get_supplier_state(supplier)
        downloaded_files = supplier_state.get('downloaded_files', [])
        
        if not downloaded_files:
            return 0
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        original_count = len(downloaded_files)
        
        # Filter out entries older than cutoff_date
        retained_files = []
        removed_count = 0
        
        for file_record in downloaded_files:
            timestamp_str = file_record.get('timestamp')
            if not timestamp_str:
                # No timestamp - keep it for safety (shouldn't happen in new records)
                retained_files.append(file_record)
                continue
            
            try:
                # Parse ISO 8601 timestamp
                file_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                
                if file_timestamp >= cutoff_date:
                    # Keep recent files
                    retained_files.append(file_record)
                else:
                    # Remove old file
                    removed_count += 1
                    logger.debug(
                        f"Removing old file entry: {file_record.get('filename')}",
                        timestamp=timestamp_str,
                        age_days=(datetime.now(timezone.utc) - file_timestamp).days
                    )
            except (ValueError, AttributeError) as e:
                # Invalid timestamp format - keep for safety
                logger.warning(
                    f"Invalid timestamp in file record: {timestamp_str}",
                    error=str(e)
                )
                retained_files.append(file_record)
        
        # Update state if any entries were removed
        if removed_count > 0:
            supplier_state['downloaded_files'] = retained_files
            self.update_supplier_state(supplier, supplier_state)
            
            logger.info(
                f"Cleaned up old file entries for {supplier}",
                removed=removed_count,
                retained=len(retained_files),
                original=original_count,
                retention_days=retention_days
            )
        else:
            logger.debug(f"No old files to clean up for {supplier}")
        
        return removed_count
    
    def store_run_results(
        self,
        execution_id: str,
        results: list,
        timestamp: str
    ) -> None:
        """
        Store run results for daily aggregation.
        
        Args:
            execution_id: Unique execution ID
            results: List of EmailProcessingResult objects
            timestamp: ISO 8601 timestamp of execution
        """
        try:
            state = self.get_state()
            if 'pending_results' not in state:
                state['pending_results'] = []
            
            # Serialize results to JSON
            results_json = self._serialize_results(results)
            
            # Calculate stats
            email_count = len(results)
            file_count = sum(len(r.files_generated) for r in results)
            
            # Store result entry
            result_entry = {
                'execution_id': execution_id,
                'timestamp': timestamp,
                'results_json': results_json,
                'email_count': email_count,
                'file_count': file_count
            }
            
            state['pending_results'].append(result_entry)
            self.update_state(state)
            
            logger.info(
                "Stored run results for daily aggregation",
                execution_id=execution_id,
                email_count=email_count,
                file_count=file_count
            )
            
        except Exception as e:
            logger.error(f"Failed to store run results: {str(e)}")
            # Don't raise - result storage is not critical
    
    def get_pending_results(self) -> list:
        """
        Get all pending results awaiting daily summary.
        
        Returns:
            List of result entry dictionaries
        """
        state = self.get_state()
        return state.get('pending_results', [])
    
    def clear_pending_results(self) -> None:
        """Clear pending results after daily summary is sent."""
        try:
            state = self.get_state()
            cleared_count = len(state.get('pending_results', []))
            state['pending_results'] = []
            self.update_state(state)
            
            logger.info(
                "Cleared pending results after daily summary",
                results_cleared=cleared_count
            )
            
        except Exception as e:
            logger.error(f"Failed to clear pending results: {str(e)}")
            # Don't raise - cleanup failure is not critical
    
    def get_last_summary_sent_timestamp(self) -> Optional[str]:
        """
        Get timestamp of last daily summary email sent.
        
        Returns:
            ISO 8601 timestamp string or None if never sent
        """
        state = self.get_state()
        return state.get('last_summary_sent_timestamp')
    
    def update_last_summary_sent(self) -> None:
        """Update timestamp of last daily summary email sent."""
        try:
            now = datetime.now(timezone.utc).isoformat()
            state = self.get_state()
            state['last_summary_sent_timestamp'] = now
            self.update_state(state)
            
            logger.info("Updated last summary sent timestamp", timestamp=now)
            
        except Exception as e:
            logger.error(f"Failed to update last summary sent timestamp: {str(e)}")
            # Don't raise - timestamp update failure is not critical
    
    def _serialize_results(self, results: list) -> str:
        """
        Serialize EmailProcessingResult objects to JSON string.
        
        Args:
            results: List of EmailProcessingResult dataclass objects
            
        Returns:
            JSON string
        """
        from dataclasses import asdict
        import json
        
        def datetime_converter(obj: Any) -> Any:
            """Convert datetime objects to ISO string."""
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj
        
        # Convert dataclasses to dicts
        results_dicts = []
        for result in results:
            result_dict = asdict(result)
            results_dicts.append(result_dict)
        
        # Serialize with datetime handling
        return json.dumps(results_dicts, default=datetime_converter)
    
    def _get_default_state(self) -> Dict[str, Any]:
        """
        Get default state for first run.
        
        Returns:
            Default state dictionary
        """
        # Default to 7 days ago to avoid processing too many old emails
        from datetime import timedelta
        default_timestamp = (
            datetime.now(timezone.utc) - timedelta(days=7)
        ).isoformat()
        
        return {
            'last_processed_timestamp': default_timestamp,
            'last_execution_timestamp': default_timestamp,
            'last_scraped': {},
            'suppliers': {},
            'last_summary_sent_timestamp': None,
            'pending_results': [],
            'version': '1.0.0'
        }
