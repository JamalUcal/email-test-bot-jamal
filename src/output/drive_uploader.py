"""
Google Drive uploader for CSV files.

Uploads generated CSV files to brand-specific Google Drive folders.
"""

from typing import Dict, Optional, List, Any
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from utils.logger import get_logger
from utils.exceptions import DriveUploadError

logger = get_logger(__name__)


class DriveUploader:
    """Uploads files to Google Drive."""
    
    # Drive API scope - using full drive scope to access existing folders
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(
        self,
        service_account_info: Dict,
        delegated_user: Optional[str] = None,
        archive_folder_name: str = "_Archive"
    ):
        """
        Initialize Drive uploader.
        
        Args:
            service_account_info: Service account credentials dictionary.
                                If empty, uses Application Default Credentials (ADC).
            delegated_user: Email address to impersonate via domain-wide delegation.
                          If None, uses service account directly (not recommended for shared folders).
            archive_folder_name: Name of subfolder for archiving old files (default: "_Archive")
        """
        self.delegated_user = delegated_user
        self.archive_folder_name = archive_folder_name
        
        try:
            # Create credentials
            if service_account_info and len(service_account_info) > 0:
                # Use provided service account credentials
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=self.SCOPES
                )
                
                # If delegated user specified, use domain-wide delegation
                if delegated_user:
                    credentials = credentials.with_subject(delegated_user)
                    logger.debug(
                        "DriveUploader initialized with domain-wide delegation",
                        delegated_user=delegated_user
                    )
                else:
                    logger.warning(
                        "DriveUploader initialized without delegation - "
                        "may fail on shared folders with domain restrictions"
                    )
            else:
                # Use Application Default Credentials for local development
                import google.auth
                
                credentials, project = google.auth.default(scopes=self.SCOPES)
                
                logger.debug(
                    "DriveUploader initialized with Application Default Credentials",
                    project=project
                )
            
            # Build Drive API service
            self.service = build('drive', 'v3', credentials=credentials)
            
            logger.debug("DriveUploader initialized successfully")
            
        except Exception as e:
            error_msg = f"Failed to initialize DriveUploader: {str(e)}"
            logger.error(error_msg, error=str(e))
            raise DriveUploadError(error_msg)
    
    def upload_file(
        self,
        file_path: str,
        folder_id: str,
        brand: str
    ) -> Dict[str, str]:
        """
        Upload file to Google Drive folder.
        
        Args:
            file_path: Local path to file
            folder_id: Google Drive folder ID
            brand: Brand name (for logging)
            
        Returns:
            Dictionary with file_id, file_name, and web_view_link
            
        Raises:
            DriveUploadError: If upload fails
        """
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                raise DriveUploadError(f"File not found: {file_path}")
            
            filename = file_path_obj.name
            
            logger.info(
                "Uploading file to Google Drive",
                filename=filename,
                folder_id=folder_id,
                brand=brand
            )
            
            # Check if file already exists in folder
            existing_file = self._check_file_exists(filename, folder_id)
            
            warning = None
            if existing_file:
                warning = f"File '{filename}' already exists in Drive folder. Creating duplicate."
                logger.warning(warning, existing_file_id=existing_file['id'])
            
            # File metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            # Upload file
            media = MediaFileUpload(
                file_path,
                mimetype='text/csv',
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            logger.info(
                "File uploaded successfully",
                file_id=file.get('id'),
                filename=filename,
                web_link=file.get('webViewLink')
            )
            
            result = {
                'file_id': file.get('id'),
                'file_name': filename,
                'web_view_link': file.get('webViewLink'),
                'folder_id': folder_id
            }
            
            if warning:
                result['warning'] = warning
            
            return result
            
        except HttpError as e:
            # Provide more helpful error messages for common issues
            if e.resp.status == 404:
                error_msg = (
                    f"Google Drive folder not found (404): {folder_id} for brand '{brand}'. "
                    f"Please verify the folder ID in brand_config.json exists and is accessible. "
                    f"File was processed successfully but not uploaded: {filename}"
                )
            else:
                error_msg = f"Google Drive API error ({e.resp.status}): {str(e)}"
            logger.error(error_msg, error=str(e), file_path=file_path, folder_id=folder_id, brand=brand)
            raise DriveUploadError(error_msg)
        except Exception as e:
            error_msg = f"Failed to upload file: {str(e)}"
            logger.error(error_msg, error=str(e), file_path=file_path)
            raise DriveUploadError(error_msg)
    
    def _check_file_exists(
        self,
        filename: str,
        folder_id: str
    ) -> Optional[Dict]:
        """
        Check if file with same name exists in folder.
        
        Args:
            filename: File name to check
            folder_id: Google Drive folder ID
            
        Returns:
            File metadata if exists, None otherwise
        """
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                file_info: Dict[str, Any] = files[0]
                return file_info
            
            return None
            
        except HttpError as e:
            logger.warning(
                f"Failed to check for existing file: {str(e)}",
                filename=filename,
                folder_id=folder_id
            )
            return None
    
    def get_folder_info(self, folder_id: str) -> Optional[Dict]:
        """
        Get folder information.
        
        Args:
            folder_id: Google Drive folder ID
            
        Returns:
            Folder metadata or None if not found
        """
        try:
            folder: Dict[str, Any] = self.service.files().get(
                fileId=folder_id,
                fields='id, name, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            return folder
            
        except HttpError as e:
            logger.error(
                f"Failed to get folder info: {str(e)}",
                folder_id=folder_id
            )
            return None
    
    def list_files_in_folder(
        self,
        folder_id: str,
        max_results: int = 10
    ) -> List[Dict]:
        """
        List files in a folder (limited results).
        
        Args:
            folder_id: Google Drive folder ID
            max_results: Maximum number of files to return
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            
            results = self.service.files().list(
                q=query,
                fields='files(id, name, createdTime, webViewLink)',
                pageSize=max_results,
                orderBy='createdTime desc',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files: List[Dict[str, Any]] = results.get('files', [])
            
            return files
            
        except HttpError as e:
            logger.error(
                f"Failed to list files: {str(e)}",
                folder_id=folder_id
            )
            return []
    
    def list_all_files_in_folder(
        self,
        folder_id: str,
        file_extension: Optional[str] = None,
        include_subfolders: bool = False
    ) -> List[Dict]:
        """
        List ALL files in a folder with pagination support.
        
        This method handles pagination to retrieve all files regardless of count.
        Used for historical data loading where folders may contain many files.
        
        Args:
            folder_id: Google Drive folder ID
            file_extension: Optional extension filter (e.g., '.csv')
            include_subfolders: If True, also list subfolders
            
        Returns:
            List of file metadata dictionaries with keys:
            - id: Google Drive file ID
            - name: File name
            - createdTime: File creation timestamp
            - modifiedTime: File modification timestamp
            - size: File size in bytes
            - mimeType: MIME type
            - webViewLink: Link to view file in Drive
        """
        all_files: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        
        try:
            # Build query
            query_parts = [
                f"'{folder_id}' in parents",
                "trashed=false"
            ]
            
            # Optionally exclude folders
            if not include_subfolders:
                query_parts.append("mimeType!='application/vnd.google-apps.folder'")
            
            query = " and ".join(query_parts)
            
            logger.debug(
                f"Listing all files in folder",
                folder_id=folder_id,
                query=query
            )
            
            while True:
                # Request a page of files
                request = self.service.files().list(
                    q=query,
                    fields='nextPageToken, files(id, name, createdTime, modifiedTime, size, mimeType, webViewLink)',
                    pageSize=1000,  # Max allowed by API
                    pageToken=page_token,
                    orderBy='name',
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                )
                
                results = request.execute()
                files = results.get('files', [])
                
                # Filter by extension if specified
                if file_extension:
                    ext = file_extension.lower()
                    if not ext.startswith('.'):
                        ext = '.' + ext
                    files = [f for f in files if f.get('name', '').lower().endswith(ext)]
                
                all_files.extend(files)
                
                # Check for more pages
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
                
                logger.debug(
                    f"Retrieved page of files, fetching more...",
                    files_so_far=len(all_files)
                )
            
            logger.info(
                f"Listed all files in folder",
                folder_id=folder_id,
                total_files=len(all_files)
            )
            
            return all_files
            
        except HttpError as e:
            logger.error(
                f"Failed to list all files: {str(e)}",
                folder_id=folder_id
            )
            return []
        except Exception as e:
            logger.error(
                f"Unexpected error listing files: {str(e)}",
                folder_id=folder_id
            )
            return []
    
    def download_file(
        self,
        file_id: str,
        destination_path: str
    ) -> str:
        """
        Download a file from Google Drive to local filesystem.
        
        Args:
            file_id: Google Drive file ID
            destination_path: Local path to save the file
            
        Returns:
            The destination path where file was saved
            
        Raises:
            DriveUploadError: If download fails
        """
        from googleapiclient.http import MediaIoBaseDownload
        import io
        
        try:
            # Get file metadata for logging
            file_metadata = self.service.files().get(
                fileId=file_id,
                fields='name, size, mimeType',
                supportsAllDrives=True
            ).execute()
            
            filename = file_metadata.get('name', 'unknown')
            file_size = file_metadata.get('size', 'unknown')
            
            logger.info(
                f"Downloading file from Drive",
                file_id=file_id,
                filename=filename,
                size=file_size,
                destination=destination_path
            )
            
            # Create request to download
            request = self.service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True
            )
            
            # Download to file
            dest_path = Path(destination_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(dest_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(
                            f"Download progress: {int(status.progress() * 100)}%",
                            file_id=file_id
                        )
            
            logger.info(
                f"File downloaded successfully",
                file_id=file_id,
                filename=filename,
                destination=destination_path
            )
            
            return str(dest_path)
            
        except HttpError as e:
            error_msg = f"Failed to download file {file_id}: {str(e)}"
            logger.error(error_msg, file_id=file_id)
            raise DriveUploadError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error downloading file {file_id}: {str(e)}"
            logger.error(error_msg, file_id=file_id)
            raise DriveUploadError(error_msg)
    
    def archive_existing_file(
        self,
        filename: str,
        folder_id: str,
        archive_folder_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Move existing file to archive folder before uploading new version.
        
        This prevents duplicates and maintains history by moving old versions
        to a designated _Archive subfolder.
        
        Args:
            filename: Name of file to archive
            folder_id: Current folder ID
            archive_folder_id: Archive folder ID (or create "_Archive" subfolder)
            
        Returns:
            Archived file ID or None if no file found
            
        Raises:
            DriveUploadError: If archiving fails
        """
        try:
            # Check if file exists
            existing_file = self._check_file_exists(filename, folder_id)
            if not existing_file:
                logger.debug(f"No existing file to archive: {filename}")
                return None
            
            # Get or create archive folder
            if not archive_folder_id:
                archive_folder_id = self._get_or_create_archive_folder(folder_id)
            
            # Move file to archive folder
            file_id = existing_file['id']
            
            logger.info(
                "Archiving existing file",
                filename=filename,
                file_id=file_id,
                archive_folder_id=archive_folder_id
            )
            
            # Update file to move it
            # Remove from current folder and add to archive folder
            updated_file = self.service.files().update(
                fileId=file_id,
                addParents=archive_folder_id,
                removeParents=folder_id,
                fields='id, name, parents',
                supportsAllDrives=True
            ).execute()
            
            logger.info(
                "File archived successfully",
                filename=filename,
                file_id=file_id,
                archive_folder_id=archive_folder_id
            )
            
            return file_id
            
        except HttpError as e:
            error_msg = f"Failed to archive file: {str(e)}"
            logger.error(error_msg, filename=filename, file_id=file_id if existing_file else None)
            raise DriveUploadError(error_msg)
        except Exception as e:
            error_msg = f"Failed to archive file: {str(e)}"
            logger.error(error_msg, filename=filename)
            raise DriveUploadError(error_msg)
    
    def _get_or_create_archive_folder(self, parent_folder_id: str) -> str:
        """
        Get or create archive subfolder within parent folder.
        
        Args:
            parent_folder_id: Parent folder ID
            
        Returns:
            Archive folder ID
            
        Raises:
            DriveUploadError: If folder creation fails
        """
        try:
            archive_folder_name = self.archive_folder_name
            
            # Check if _Archive folder already exists
            query = (
                f"name='{archive_folder_name}' and "
                f"'{parent_folder_id}' in parents and "
                f"mimeType='application/vnd.google-apps.folder' and "
                f"trashed=false"
            )
            
            results = self.service.files().list(
                q=query,
                fields='files(id, name)',
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                # Archive folder exists
                archive_folder_id = files[0]['id']
                logger.debug(
                    f"Using existing archive folder: {archive_folder_name}",
                    archive_folder_id=archive_folder_id
                )
                return archive_folder_id
            
            # Create archive folder
            logger.info(
                f"Creating archive subfolder: {archive_folder_name}",
                parent_folder_id=parent_folder_id
            )
            
            folder_metadata = {
                'name': archive_folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            
            folder = self.service.files().create(
                body=folder_metadata,
                fields='id, name',
                supportsAllDrives=True
            ).execute()
            
            archive_folder_id = folder.get('id')
            
            logger.info(
                f"Archive folder created successfully: {archive_folder_name}",
                archive_folder_id=archive_folder_id,
                parent_folder_id=parent_folder_id
            )
            
            return archive_folder_id
            
        except HttpError as e:
            error_msg = f"Failed to get/create archive folder: {str(e)}"
            logger.error(error_msg, parent_folder_id=parent_folder_id)
            raise DriveUploadError(error_msg)
        except Exception as e:
            error_msg = f"Failed to get/create archive folder: {str(e)}"
            logger.error(error_msg, parent_folder_id=parent_folder_id)
            raise DriveUploadError(error_msg)
    
    def upload_file_with_archive(
        self,
        file_path: str,
        folder_id: str,
        brand: str,
        archive_old: bool = True
    ) -> Dict[str, str]:
        """
        Upload file to Google Drive, optionally archiving existing version.
        
        Args:
            file_path: Local path to file
            folder_id: Google Drive folder ID
            brand: Brand name (for logging)
            archive_old: If True, archive existing file before uploading new version
            
        Returns:
            Dictionary with file_id, file_name, web_view_link, and optional archived_file_id
            
        Raises:
            DriveUploadError: If upload or archiving fails
        """
        file_path_obj = Path(file_path)
        filename = file_path_obj.name
        
        result = {
            'file_id': '',
            'file_name': filename,
            'web_view_link': '',
            'folder_id': folder_id
        }
        
        # Archive existing file if requested
        if archive_old:
            try:
                archived_id = self.archive_existing_file(filename, folder_id)
                if archived_id:
                    result['archived_file_id'] = archived_id
                    result['warning'] = f"Archived previous version: {filename}"
                    logger.info(f"Archived previous version", filename=filename, archived_id=archived_id)
            except DriveUploadError as e:
                # Log warning but continue with upload
                logger.warning(f"Failed to archive existing file: {e}", filename=filename)
                result['warning'] = f"Could not archive previous version: {str(e)}"
        
        # Upload new file
        upload_result = self.upload_file(file_path, folder_id, brand)
        result.update(upload_result)
        
        return result
    
    def delete_file(self, file_id: str) -> bool:
        """
        Delete a file from Google Drive (moves to trash).
        
        Args:
            file_id: Google Drive file ID
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            # Get file info first
            try:
                file_info = self.service.files().get(
                    fileId=file_id,
                    fields='id, name, driveId',
                    supportsAllDrives=True
                ).execute()
                logger.info(f"Found file: {file_info.get('name')}", file_id=file_id)
            except HttpError as get_err:
                logger.error(
                    f"Cannot access file: {get_err.resp.status} - {get_err._get_reason()}",
                    file_id=file_id
                )
                return False
            
            # Move to trash (works better than delete for Shared Drives)
            self.service.files().update(
                fileId=file_id,
                body={'trashed': True},
                supportsAllDrives=True
            ).execute()
            
            logger.info("File trashed successfully", file_id=file_id)
            return True
            
        except HttpError as e:
            logger.error(
                f"Trash failed: {e.resp.status} - {e._get_reason()} - {e.error_details}",
                file_id=file_id
            )
            return False
        except Exception as e:
            logger.error(f"Failed to trash file: {str(e)}", file_id=file_id)
            return False
    
    def delete_files_by_urls(self, urls: List[str]) -> Dict[str, Any]:
        """
        Delete multiple files from Google Drive by URL.
        
        Args:
            urls: List of Google Drive file URLs
            
        Returns:
            Dictionary with success_count, fail_count, and details
        """
        import re
        
        success_count = 0
        fail_count = 0
        results = []
        
        for url in urls:
            # Extract file ID from URL
            match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
            if not match:
                logger.warning(f"Could not extract file ID from URL: {url}")
                results.append({'url': url, 'status': 'invalid_url'})
                fail_count += 1
                continue
            
            file_id = match.group(1)
            
            if self.delete_file(file_id):
                results.append({'url': url, 'file_id': file_id, 'status': 'deleted'})
                success_count += 1
            else:
                results.append({'url': url, 'file_id': file_id, 'status': 'failed'})
                fail_count += 1
        
        return {
            'success_count': success_count,
            'fail_count': fail_count,
            'total': len(urls),
            'results': results
        }