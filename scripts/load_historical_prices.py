#!/usr/bin/env python3
"""
Historical Price Loader - Load historical pricing files from Google Drive into BigQuery.

This script downloads historical pricing files from Google Drive folders specified in
a dedicated config file (config/historical/historical_loader_config.json), parses the
standardized filename to extract metadata, and loads them into BigQuery with proper
handling for out-of-sequence data.

Process flow:
1. Load historical loader config (folder IDs from historical_loader_config.json)
2. For each folder (one at a time to reduce memory usage):
   a. List CSV files from Drive folder
   b. For each file:
      - If filename is invalid, record in tracking with error message
      - If filename is valid, add to tracking as pending
   c. Process pending files in that folder
   d. Move to next folder
3. Track all results in local JSON tracking file
4. Output summary

Usage:
    python scripts/load_historical_prices.py --dry-run
    python scripts/load_historical_prices.py --folder TOYOTA --max-files 10
    python scripts/load_historical_prices.py --resume
    python scripts/load_historical_prices.py --config path/to/custom_config.json
"""

import sys
import os
import argparse
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.logger import setup_logger
from utils.filename_parser import parse_standard_filename, ParsedFilename, is_valid_pricing_filename
from output.drive_uploader import DriveUploader
from storage.bigquery_processor import BigQueryPriceListProcessor
from parsers.price_list_parser import PriceListParser
from output.file_generator import FileGenerator

logger = setup_logger(__name__)


# =============================================================================
# Tracking System
# =============================================================================

@dataclass
class FileTrackingEntry:
    """Tracking entry for a single file."""
    filename: str
    drive_file_id: str
    status: str  # pending, completed, failed, skipped, invalid_filename
    processed_at: Optional[str] = None
    price_list_id: Optional[str] = None
    error: Optional[str] = None
    brand: Optional[str] = None
    supplier: Optional[str] = None
    valid_from_date: Optional[str] = None
    folder_name: Optional[str] = None  # Which folder this file came from
    

@dataclass
class TrackingState:
    """Tracking state for the historical loader."""
    last_run: Optional[str] = None
    total_files_found: int = 0
    files: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def get_entry(self, filename: str) -> Optional[FileTrackingEntry]:
        """Get tracking entry for a file."""
        if filename in self.files:
            return FileTrackingEntry(**self.files[filename])
        return None
    
    def set_entry(self, entry: FileTrackingEntry) -> None:
        """Set tracking entry for a file."""
        self.files[entry.filename] = asdict(entry)
    
    def get_pending_files(self) -> List[str]:
        """Get list of files that haven't been processed yet."""
        return [
            fname for fname, data in self.files.items()
            if data.get('status') == 'pending'
        ]
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about processed files."""
        stats = {'pending': 0, 'completed': 0, 'failed': 0, 'skipped': 0, 'invalid_filename': 0}
        for data in self.files.values():
            status = data.get('status', 'unknown')
            if status in stats:
                stats[status] += 1
        return stats


class TrackingManager:
    """Manages the JSON-based tracking file."""
    
    def __init__(self, tracking_file_path: str):
        """
        Initialize tracking manager.
        
        Args:
            tracking_file_path: Path to the tracking JSON file
        """
        self.tracking_file_path = Path(tracking_file_path)
        self.state = TrackingState()
        
        # Create parent directory if needed
        self.tracking_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load(self) -> TrackingState:
        """Load tracking state from file."""
        if self.tracking_file_path.exists():
            try:
                with open(self.tracking_file_path, 'r') as f:
                    data = json.load(f)
                    self.state = TrackingState(
                        last_run=data.get('last_run'),
                        total_files_found=data.get('total_files_found', 0),
                        files=data.get('files', {})
                    )
                    logger.info(
                        f"Loaded tracking state",
                        tracking_file=str(self.tracking_file_path),
                        total_files=len(self.state.files)
                    )
            except Exception as e:
                logger.warning(f"Failed to load tracking state: {e}")
                self.state = TrackingState()
        return self.state
    
    def save(self) -> None:
        """Save tracking state to file."""
        try:
            self.state.last_run = datetime.now(timezone.utc).isoformat()
            with open(self.tracking_file_path, 'w') as f:
                json.dump({
                    'last_run': self.state.last_run,
                    'total_files_found': self.state.total_files_found,
                    'files': self.state.files
                }, f, indent=2, default=str)
            logger.debug(f"Saved tracking state to {self.tracking_file_path}")
        except Exception as e:
            logger.error(f"Failed to save tracking state: {e}")
    
    def add_file(
        self,
        filename: str,
        drive_file_id: str,
        parsed: Optional[ParsedFilename] = None,
        folder_name: Optional[str] = None
    ) -> None:
        """Add a new file to tracking (if not already present)."""
        if filename not in self.state.files:
            entry = FileTrackingEntry(
                filename=filename,
                drive_file_id=drive_file_id,
                status='pending',
                brand=parsed.brand if parsed else None,
                supplier=parsed.supplier if parsed else None,
                valid_from_date=parsed.valid_from_date.isoformat() if parsed else None,
                folder_name=folder_name
            )
            self.state.set_entry(entry)
    
    def mark_completed(
        self,
        filename: str,
        price_list_id: str
    ) -> None:
        """Mark a file as successfully processed."""
        entry = self.state.get_entry(filename)
        if entry:
            entry.status = 'completed'
            entry.processed_at = datetime.now(timezone.utc).isoformat()
            entry.price_list_id = price_list_id
            entry.error = None
            self.state.set_entry(entry)
    
    def mark_failed(self, filename: str, error: str) -> None:
        """Mark a file as failed."""
        entry = self.state.get_entry(filename)
        if entry:
            entry.status = 'failed'
            entry.processed_at = datetime.now(timezone.utc).isoformat()
            entry.error = error
            self.state.set_entry(entry)
    
    def mark_skipped(self, filename: str, reason: str) -> None:
        """Mark a file as skipped."""
        entry = self.state.get_entry(filename)
        if entry:
            entry.status = 'skipped'
            entry.processed_at = datetime.now(timezone.utc).isoformat()
            entry.error = reason
            self.state.set_entry(entry)
    
    def mark_invalid_filename(
        self,
        filename: str,
        drive_file_id: str,
        error: str,
        folder_name: Optional[str] = None
    ) -> None:
        """
        Mark a file as having an invalid/unparseable filename.
        
        This adds the file to tracking so users can review and correct the filename.
        
        Args:
            filename: The filename that couldn't be parsed
            drive_file_id: Google Drive file ID
            error: The parsing error message
            folder_name: The folder this file was found in
        """
        entry = FileTrackingEntry(
            filename=filename,
            drive_file_id=drive_file_id,
            status='invalid_filename',
            processed_at=datetime.now(timezone.utc).isoformat(),
            error=error,
            folder_name=folder_name
        )
        self.state.set_entry(entry)


# =============================================================================
# Configuration Loading
# =============================================================================

def load_historical_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load historical loader configuration from JSON file.
    
    Args:
        config_path: Optional path to config file. If not provided,
                     uses default: config/historical/historical_loader_config.json
                     
    Returns:
        Dictionary with historical loader config including:
        - folders: List of folder configs with name, folder_id, enabled
        - tracking_file: Path to tracking file
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = Path(__file__).parent.parent / "config" / "historical" / "historical_loader_config.json"
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"Historical loader config not found: {config_file}\n"
            f"Please create this file with your Drive folder IDs."
        )
    
    logger.info(f"Loading historical config from: {config_file}")
    with open(config_file) as f:
        config = json.load(f)
    
    # Validate required fields
    if 'folders' not in config:
        raise ValueError("Historical config must contain 'folders' array")
    
    # Log folder count
    enabled_folders = [f for f in config['folders'] if f.get('enabled', True)]
    logger.info(f"Found {len(enabled_folders)} enabled folders in config")
    
    return config


def load_configs(use_test_config: bool = False) -> Dict[str, Any]:
    """
    Load core configuration files from local filesystem.
    
    Note: This no longer loads brand config for folder mapping - that's handled
    by load_historical_config(). This function loads configs needed for
    processing files (supplier config, column mapping, etc.)
    
    Args:
        use_test_config: If True, load test configurations
        
    Returns:
        Dictionary with all configs
    """
    config_dir = Path(__file__).parent.parent / "config"
    
    # Load core config
    core_filename = "core_config_test.json" if use_test_config else "core_config.json"
    core_file = config_dir / "core" / core_filename
    
    # Try production config first, fall back to test
    if not core_file.exists():
        core_file = config_dir / "core" / "core_config_production.json"
    
    logger.info(f"Loading core config from: {core_file.name}")
    with open(core_file) as f:
        core_config = json.load(f)
    
    # Load supplier config
    with open(config_dir / "supplier" / "supplier_config.json") as f:
        supplier_config = json.load(f)
    
    # Load brand config (still needed for brand-specific settings like minimumPartLength)
    brand_filename = "brand_config_test.json" if use_test_config else "brand_config.json"
    brand_file = config_dir / "brand" / brand_filename
    logger.info(f"Loading brand config from: {brand_filename}")
    with open(brand_file) as f:
        brand_config = json.load(f)
    
    # Load column mapping config
    column_mapping_file = config_dir / "core" / "column_mapping_config.json"
    with open(column_mapping_file) as f:
        column_mapping_config = json.load(f)
    
    return {
        'core': core_config,
        'suppliers': supplier_config,
        'brands': brand_config,
        'column_mapping': column_mapping_config
    }


def find_supplier_config(
    supplier_name: str,
    brand_name: str,
    supplier_configs: List[Dict]
) -> Optional[Dict]:
    """
    Find supplier config matching supplier name and brand.
    
    Args:
        supplier_name: Supplier name from filename
        brand_name: Brand name from filename
        supplier_configs: List of supplier configurations
        
    Returns:
        Supplier config dict or None if not found
    """
    supplier_name_upper = supplier_name.upper()
    
    for supplier in supplier_configs:
        if supplier.get('supplier', '').upper() == supplier_name_upper:
            return supplier
    
    # Try partial match
    for supplier in supplier_configs:
        if supplier_name_upper in supplier.get('supplier', '').upper():
            return supplier
        if supplier.get('supplier', '').upper() in supplier_name_upper:
            return supplier
    
    return None


def find_brand_config_in_supplier(
    brand_name: str,
    supplier_config: Dict
) -> Optional[Dict]:
    """
    Find brand config within a supplier config.
    
    Args:
        brand_name: Brand name to find
        supplier_config: Supplier configuration
        
    Returns:
        Brand config dict or None if not found
    """
    brand_name_upper = brand_name.upper()
    
    for brand_config in supplier_config.get('config', []):
        if brand_config.get('brand', '').upper() == brand_name_upper:
            return brand_config
        # Check for brand with underscores (e.g., VAG_OIL)
        if brand_name_upper.startswith(brand_config.get('brand', '').upper()):
            return brand_config
    
    return None


# =============================================================================
# Main Processing
# =============================================================================

def process_single_file(
    file_info: Dict[str, Any],
    configs: Dict[str, Any],
    drive_uploader: DriveUploader,
    bq_processor: BigQueryPriceListProcessor,
    tracking_manager: TrackingManager,
    temp_dir: Path,
    stats: Dict[str, int]
) -> bool:
    """
    Process a single file.
    
    Args:
        file_info: File info dict with filename, file_id, folder_name
        configs: Configuration dict
        drive_uploader: Drive uploader instance
        bq_processor: BigQuery processor instance
        tracking_manager: Tracking manager instance
        temp_dir: Temporary directory for downloads
        stats: Statistics dict to update
        
    Returns:
        True if processing should continue, False if should stop
    """
    filename = file_info['filename']
    
    try:
        # Parse filename
        parsed = parse_standard_filename(filename)
        
        # Check if already in BigQuery
        existing = bq_processor.check_price_list_exists(
            supplier=parsed.supplier,
            brand=parsed.brand,
            valid_from_date=parsed.valid_from_date
        )
        
        if existing:
            logger.info(
                f"Skipping {filename}: already exists in BigQuery",
                price_list_id=existing.get('price_list_id')
            )
            tracking_manager.mark_skipped(
                filename,
                f"Already in BigQuery: {existing.get('price_list_id')}"
            )
            stats['files_skipped'] += 1
            tracking_manager.save()
            return True
        
        # Download file
        local_path = temp_dir / filename
        drive_uploader.download_file(
            file_id=file_info['file_id'],
            destination_path=str(local_path)
        )
        
        # Find supplier and brand configs
        supplier_config = find_supplier_config(
            parsed.supplier,
            parsed.brand,
            configs['suppliers']
        )
        
        if not supplier_config:
            raise ValueError(f"No supplier config found for: {parsed.supplier}")
        
        brand_config = find_brand_config_in_supplier(parsed.brand, supplier_config)
        
        if not brand_config:
            # Try to find from brand_configs directly
            for bc in configs['brands']:
                if bc.get('brand', '').upper() == parsed.brand.upper():
                    brand_config = {
                        'brand': bc.get('brand'),
                        'location': parsed.location,
                        'currency': parsed.currency,
                        'decimalFormat': supplier_config.get('decimalFormat', 'Decimal'),
                        'minimumPartLength': bc.get('minimumPartLength', 10)
                    }
                    break
        
        if not brand_config:
            raise ValueError(f"No brand config found for: {parsed.brand}")
        
        # Ensure brand_config has required fields
        if 'location' not in brand_config:
            brand_config['location'] = parsed.location
        if 'currency' not in brand_config:
            brand_config['currency'] = parsed.currency
        if 'decimalFormat' not in brand_config:
            brand_config['decimalFormat'] = supplier_config.get('decimalFormat', 'Decimal')
        
        # Process through BigQuery
        price_list_id, gcs_path = bq_processor.process_price_list(
            local_csv_path=str(local_path),
            supplier=parsed.supplier,
            brand=parsed.brand,
            currency=parsed.currency,
            location=parsed.location,
            source_filename=filename,
            valid_from_date=parsed.valid_from_date,
            source_email_subject=f"Historical load: {filename}",
            source_email_date=datetime.now(timezone.utc)
        )
        
        logger.info(
            f"Successfully processed {filename}",
            price_list_id=price_list_id
        )
        
        tracking_manager.mark_completed(filename, price_list_id)
        stats['files_processed'] += 1
        
        # Cleanup local file
        if local_path.exists():
            local_path.unlink()
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to process {filename}: {error_msg}")
        tracking_manager.mark_failed(filename, error_msg)
        stats['files_failed'] += 1
        return True  # Continue by default, caller handles --continue-on-error


def process_historical_files(
    args: argparse.Namespace,
    historical_config: Dict[str, Any],
    configs: Dict[str, Any],
    drive_uploader: DriveUploader,
    bq_processor: BigQueryPriceListProcessor,
    tracking_manager: TrackingManager
) -> Dict[str, int]:
    """
    Main processing loop for historical files.
    
    Processes one folder at a time to reduce memory usage.
    
    Args:
        args: Command line arguments
        historical_config: Historical loader config with folder list
        configs: Core configs (supplier, brand, etc.)
        drive_uploader: Drive uploader instance
        bq_processor: BigQuery processor instance
        tracking_manager: Tracking manager instance
        
    Returns:
        Statistics dict with counts
    """
    stats = {
        'files_found': 0,
        'files_processed': 0,
        'files_skipped': 0,
        'files_failed': 0,
        'files_invalid_filename': 0,
        'folders_processed': 0
    }
    
    # Get enabled folders from config
    folders = [
        f for f in historical_config.get('folders', [])
        if f.get('enabled', True)
    ]
    
    # Filter folders if --folder specified
    if args.folder:
        folder_filter = args.folder.upper()
        folders = [
            f for f in folders
            if f.get('name', '').upper() == folder_filter 
            or f.get('name', '').upper().startswith(folder_filter)
        ]
        if not folders:
            logger.error(f"No folder found matching: {args.folder}")
            return stats
    
    logger.info(f"Will process {len(folders)} folders")
    
    # Track total files across all folders for max_files limit
    total_files_processed = 0
    max_files = args.max_files
    should_stop = False
    
    # Create temp directory for downloads
    temp_dir = Path(tempfile.mkdtemp(prefix='historical_prices_'))
    logger.info(f"Using temp directory: {temp_dir}")
    
    try:
        # Process one folder at a time
        for folder_idx, folder_config in enumerate(folders, 1):
            if should_stop:
                break
                
            folder_name = folder_config.get('name', 'Unknown')
            folder_id = folder_config.get('folder_id')
            
            if not folder_id:
                logger.warning(f"Folder '{folder_name}' has no folder_id, skipping")
                continue
            
            logger.info(f"[{folder_idx}/{len(folders)}] Processing folder: {folder_name}")
            logger.info(f"  Folder ID: {folder_id}")
            
            # List files in this folder
            try:
                files = drive_uploader.list_all_files_in_folder(
                    folder_id=folder_id,
                    file_extension='.csv'
                )
            except Exception as e:
                logger.error(f"Failed to list files in {folder_name} folder: {e}")
                continue
            
            logger.info(f"  Found {len(files)} CSV files in folder")
            
            # Process files in this folder
            folder_files: List[Dict[str, Any]] = []
            
            for file_info in files:
                filename = file_info.get('name', '')
                file_id = file_info.get('id')
                
                # Skip files without an ID
                if not file_id:
                    logger.warning(f"  Skipping file with no ID: {filename}")
                    continue
                
                # Check if already in tracking
                existing_entry = tracking_manager.state.get_entry(filename)
                if existing_entry:
                    # Already tracked - skip discovery
                    if existing_entry.status == 'pending':
                        folder_files.append({
                            'filename': filename,
                            'file_id': file_id,
                            'folder_name': folder_name
                        })
                    continue
                
                # Check if valid pricing filename
                if not is_valid_pricing_filename(filename):
                    # Get the specific error message for tracking
                    try:
                        parse_standard_filename(filename)
                        error_msg = "Unknown parsing error"
                    except ValueError as e:
                        error_msg = str(e)
                    
                    # Record in tracking file so user can review and fix
                    tracking_manager.mark_invalid_filename(
                        filename=filename,
                        drive_file_id=file_id,
                        error=error_msg,
                        folder_name=folder_name
                    )
                    logger.warning(f"  Invalid filename recorded: {filename}")
                    logger.warning(f"    Error: {error_msg}")
                    stats['files_invalid_filename'] += 1
                    continue
                
                # Valid filename - add to tracking as pending
                try:
                    parsed = parse_standard_filename(filename)
                    tracking_manager.add_file(
                        filename=filename,
                        drive_file_id=file_id,
                        parsed=parsed,
                        folder_name=folder_name
                    )
                except ValueError:
                    # Should not happen since we checked is_valid_pricing_filename
                    pass
                
                folder_files.append({
                    'filename': filename,
                    'file_id': file_id,
                    'folder_name': folder_name
                })
                stats['files_found'] += 1
            
            # Save tracking state after folder discovery
            tracking_manager.save()
            
            # Filter to pending files only
            files_to_process = [
                f for f in folder_files
                if (entry := tracking_manager.state.get_entry(f['filename'])) is not None
                   and entry.status == 'pending'
            ]
            
            logger.info(f"  {len(files_to_process)} files pending in this folder")
            
            if args.dry_run:
                logger.info("  DRY RUN - Listing files without processing:")
                for f in folder_files:
                    entry = tracking_manager.state.get_entry(f['filename'])
                    status = entry.status if entry else 'unknown'
                    logger.info(f"    [{status}] {f['filename']}")
                stats['folders_processed'] += 1
                continue
            
            # Process files in this folder
            for file_idx, file_info in enumerate(files_to_process, 1):
                # Check max_files limit
                if max_files and total_files_processed >= max_files:
                    logger.info(f"Reached max_files limit ({max_files})")
                    should_stop = True
                    break
                
                filename = file_info['filename']
                logger.info(f"  [{file_idx}/{len(files_to_process)}] Processing: {filename}")
                
                success = process_single_file(
                    file_info=file_info,
                    configs=configs,
                    drive_uploader=drive_uploader,
                    bq_processor=bq_processor,
                    tracking_manager=tracking_manager,
                    temp_dir=temp_dir,
                    stats=stats
                )
                
                total_files_processed += 1
                
                # Save tracking state after each file
                tracking_manager.save()
                
                # Check if we should stop on error
                if not success and not args.continue_on_error:
                    logger.error("Stopping due to error (use --continue-on-error to continue)")
                    should_stop = True
                    break
            
            stats['folders_processed'] += 1
            logger.info(f"  Completed folder: {folder_name}")
    
    finally:
        # Cleanup temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    # Update total files found in tracking
    tracking_manager.state.total_files_found = stats['files_found']
    
    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load historical pricing files from Google Drive into BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all files without processing (dry run)
  python scripts/load_historical_prices.py --dry-run

  # Process specific folder only
  python scripts/load_historical_prices.py --folder TOYOTA

  # Process with limit
  python scripts/load_historical_prices.py --max-files 10

  # Resume from tracking file
  python scripts/load_historical_prices.py --resume

  # Continue processing even after errors
  python scripts/load_historical_prices.py --continue-on-error

  # Use custom config file
  python scripts/load_historical_prices.py --config path/to/custom_config.json

  # Use test configuration for core configs
  python scripts/load_historical_prices.py --use-test-config --dry-run
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing'
    )
    
    parser.add_argument(
        '--folder',
        type=str,
        help='Process only specific folder by name (e.g., TOYOTA, VAG)'
    )
    
    parser.add_argument(
        '--max-files',
        type=int,
        help='Maximum number of files to process (across all folders)'
    )
    
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from tracking file (skip already processed)'
    )
    
    parser.add_argument(
        '--continue-on-error',
        action='store_true',
        help='Continue processing after errors'
    )
    
    parser.add_argument(
        '--use-test-config',
        action='store_true',
        help='Use test configurations for core/supplier configs'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to historical loader config file (default: config/historical/historical_loader_config.json)'
    )
    
    parser.add_argument(
        '--tracking-file',
        type=str,
        help='Override path to tracking file (default: from config or ./state/historical_load_tracking.json)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Historical Price Loader starting...")
    logger.info("=" * 60)
    
    try:
        # Load historical loader config first (for folder IDs and default tracking file)
        historical_config = load_historical_config(args.config)
        
        # Determine tracking file path (CLI arg > config > default)
        tracking_file = (
            args.tracking_file or 
            historical_config.get('tracking_file') or 
            './state/historical_load_tracking.json'
        )
        
        logger.info(f"  Config file: {args.config or 'default'}")
        logger.info(f"  Dry run: {args.dry_run}")
        logger.info(f"  Folder filter: {args.folder or 'all'}")
        logger.info(f"  Max files: {args.max_files or 'unlimited'}")
        logger.info(f"  Resume: {args.resume}")
        logger.info(f"  Continue on error: {args.continue_on_error}")
        logger.info(f"  Tracking file: {tracking_file}")
        
        # Load core configurations (supplier, brand, etc.)
        configs = load_configs(use_test_config=args.use_test_config)
        
        # Load service account credentials
        service_account_info = {}
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        if credentials_path and os.path.exists(credentials_path):
            logger.info(f"Loading credentials from: {credentials_path}")
            with open(credentials_path) as f:
                service_account_info = json.load(f)
        else:
            logger.warning(
                "No GOOGLE_APPLICATION_CREDENTIALS set, using Application Default Credentials"
            )
        
        # Initialize Drive uploader
        delegated_user = configs['core'].get('gmail', {}).get('delegated_user_email')
        drive_uploader = DriveUploader(
            service_account_info=service_account_info,
            delegated_user=delegated_user
        )
        
        # Initialize BigQuery processor
        bq_config = configs['core'].get('bigquery', {})
        bq_processor = BigQueryPriceListProcessor(
            project_id=bq_config.get('project_id', 'pricing-email-bot'),
            dataset_id=bq_config.get('dataset_id', 'PRICING'),
            staging_bucket=bq_config.get('staging_bucket', 'pricing-email-bot-bucket'),
            service_account_info=service_account_info,
            bigquery_config=bq_config
        )
        
        # Initialize tracking manager
        tracking_manager = TrackingManager(tracking_file)
        
        # Load existing tracking state if resuming
        if args.resume:
            tracking_manager.load()
        
        # Process files
        stats = process_historical_files(
            args=args,
            historical_config=historical_config,
            configs=configs,
            drive_uploader=drive_uploader,
            bq_processor=bq_processor,
            tracking_manager=tracking_manager
        )
        
        # Final save
        tracking_manager.save()
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info("=" * 60)
        logger.info(f"  Folders processed: {stats['folders_processed']}")
        logger.info(f"  Files found: {stats['files_found']}")
        logger.info(f"  Files processed: {stats['files_processed']}")
        logger.info(f"  Files skipped: {stats['files_skipped']}")
        logger.info(f"  Files failed: {stats['files_failed']}")
        logger.info(f"  Invalid filenames: {stats['files_invalid_filename']}")
        
        tracking_stats = tracking_manager.state.get_stats()
        logger.info(f"  Tracking state: {tracking_stats}")
        
        # Exit with error code if there were failures
        if stats['files_failed'] > 0:
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
