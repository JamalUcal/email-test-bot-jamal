#!/usr/bin/env python3
"""
Script to delete Google Drive files.

Usage:
    python scripts/delete_drive_files.py

URLs are read from config/delete/delete_file_url.txt (one URL per line).
"""

import sys
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.drive_uploader import DriveUploader
from utils.logger import setup_logger

logger = setup_logger(__name__)


def load_local_config():
    """Load core config from local filesystem."""
    config_dir = Path(__file__).parent.parent / "config"
    core_file = config_dir / "core" / "core_config.json"
    
    with open(core_file) as f:
        return json.load(f)


def load_urls_to_delete():
    """Load URLs from config/delete/delete_file_url.txt."""
    config_file = Path(__file__).parent.parent / "config" / "delete" / "delete_file_url.txt"
    
    if not config_file.exists():
        logger.error(f"URL file not found: {config_file}")
        sys.exit(1)
    
    with open(config_file) as f:
        # Read lines, strip whitespace, filter empty lines
        urls = [line.strip() for line in f if line.strip()]
    
    return urls


def main():
    # Load URLs from config file
    urls = load_urls_to_delete()
    
    if not urls:
        print("No URLs found in config/delete/delete_file_url.txt")
        return
    
    print(f"About to delete {len(urls)} files from Google Drive...")
    response = input("Are you sure? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Aborted.")
        return
    
    # Load config from local filesystem
    core_config = load_local_config()
    
    # Load service account credentials from GOOGLE_APPLICATION_CREDENTIALS env var
    service_account_info = {}
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if credentials_path and os.path.exists(credentials_path):
        logger.info(f"Loading service account from: {credentials_path}")
        with open(credentials_path) as f:
            service_account_info = json.load(f)
    else:
        logger.error(
            "No service account credentials found. "
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
        sys.exit(1)
    
    # Initialize DriveUploader
    delegated_user = core_config['drive']['impersonation_email']
    logger.info(f"Initializing DriveUploader (delegated user: {delegated_user})")
    
    uploader = DriveUploader(
        service_account_info=service_account_info,
        delegated_user=delegated_user
    )
    
    # Delete files
    logger.info("Deleting files...")
    result = uploader.delete_files_by_urls(urls)
    
    # Print results
    for r in result['results']:
        status = r['status']
        file_id = r.get('file_id', 'N/A')
        if status == 'deleted':
            print(f"✅ Trashed: {file_id}")
        elif status == 'invalid_url':
            print(f"⚠️  Invalid URL: {r['url']}")
        else:
            print(f"❌ Failed: {file_id}")
    
    print(f"\n{'='*50}")
    print(f"Summary: {result['success_count']} trashed, {result['fail_count']} failed")
    print(f"Total files processed: {result['total']}")


if __name__ == '__main__':
    main()
