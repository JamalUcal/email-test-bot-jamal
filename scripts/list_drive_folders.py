#!/usr/bin/env python3
"""
List folders in a Google Drive folder.

Usage:
    python list_drive_folders.py PARENT_FOLDER_ID
"""

import sys
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account

def list_folders(parent_folder_id, service_account_file):
    """List all folders in a parent folder."""
    
    # Load service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    
    # Delegate to a user if needed (for Shared Drives)
    # Uncomment and set if using domain-wide delegation
    # credentials = credentials.with_subject('user@domain.com')
    
    # Build Drive service
    service = build('drive', 'v3', credentials=credentials)
    
    # Query for folders
    query = f"mimeType='application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed=false"
    
    try:
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=100
        ).execute()
        
        folders = results.get('files', [])
        
        if not folders:
            print(f"No folders found in parent folder: {parent_folder_id}")
            return
        
        print(f"\nFound {len(folders)} folders:\n")
        print(f"{'Folder Name':<40} {'Folder ID'}")
        print("-" * 90)
        
        for folder in sorted(folders, key=lambda x: x['name']):
            print(f"{folder['name']:<40} {folder['id']}")
        
        # Also output as JSON for easy copying
        print("\n\nJSON format:")
        print(json.dumps(folders, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python list_drive_folders.py PARENT_FOLDER_ID [SERVICE_ACCOUNT_KEY_FILE]")
        print("\nExample:")
        print("  python list_drive_folders.py 1yRCxKpgl4jhqXAtVzzlTlfdHC8mCzQj0")
        sys.exit(1)
    
    parent_folder_id = sys.argv[1]
    service_account_file = sys.argv[2] if len(sys.argv) > 2 else f"{sys.path[0]}/../email-pricing-bot-key.json"
    
    # Try to find service account key
    import os
    if not os.path.exists(service_account_file):
        service_account_file = os.path.expanduser("~/email-pricing-bot-key.json")
    
    if not os.path.exists(service_account_file):
        print(f"Error: Service account key file not found: {service_account_file}")
        print("\nPlease provide the path to your service account key file:")
        print("  python list_drive_folders.py FOLDER_ID /path/to/key.json")
        sys.exit(1)
    
    list_folders(parent_folder_id, service_account_file)
