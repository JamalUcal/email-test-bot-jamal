"""
Email attachment handler.

Detects, downloads, and manages email attachments.
"""

import os
import tempfile
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from utils.logger import get_logger
from utils.exceptions import EmailError

logger = get_logger(__name__)


@dataclass
class Attachment:
    """Attachment metadata."""
    filename: str
    mime_type: str
    size: int
    attachment_id: str
    message_id: str
    local_path: Optional[str] = None


class AttachmentHandler:
    """Handles email attachment detection and download."""
    
    # Supported file extensions
    SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.xlsb', '.xls'}
    WARNING_EXTENSIONS = {'.pdf'}
    
    def __init__(self, gmail_client):
        """
        Initialize attachment handler.
        
        Args:
            gmail_client: GmailClient instance
        """
        self.gmail_client = gmail_client
        self.temp_dir = None
    
    def list_attachments(self, message: Dict) -> List[Attachment]:
        """
        List all attachments in a message.
        
        Args:
            message: Gmail message object
            
        Returns:
            List of Attachment objects
        """
        attachments = []
        message_id = message['id']
        
        def process_part(part: Dict):
            """Recursively process message parts."""
            if 'parts' in part:
                for subpart in part['parts']:
                    process_part(subpart)
            
            # Check if part is an attachment
            if 'filename' in part and part['filename']:
                body = part.get('body', {})
                attachment_id = body.get('attachmentId')
                
                if attachment_id:
                    attachment = Attachment(
                        filename=part['filename'],
                        mime_type=part.get('mimeType', 'application/octet-stream'),
                        size=body.get('size', 0),
                        attachment_id=attachment_id,
                        message_id=message_id
                    )
                    attachments.append(attachment)
        
        # Process message payload
        if 'payload' in message:
            process_part(message['payload'])
        
        logger.info(
            "Attachments listed",
            message_id=message_id,
            count=len(attachments),
            filenames=[a.filename for a in attachments]
        )
        
        return attachments
    
    def filter_attachments(
        self,
        attachments: List[Attachment]
    ) -> Tuple[List[Attachment], List[Attachment], List[Attachment]]:
        """
        Filter attachments by type.
        
        Args:
            attachments: List of attachments
            
        Returns:
            Tuple of (supported, warning, ignored) attachment lists
        """
        supported = []
        warning = []
        ignored = []
        
        for attachment in attachments:
            ext = os.path.splitext(attachment.filename)[1].lower()
            
            if ext in self.SUPPORTED_EXTENSIONS:
                supported.append(attachment)
            elif ext in self.WARNING_EXTENSIONS:
                warning.append(attachment)
            else:
                ignored.append(attachment)
        
        logger.info(
            "Attachments filtered",
            supported=len(supported),
            warning=len(warning),
            ignored=len(ignored)
        )
        
        return supported, warning, ignored
    
    def download_attachment(
        self,
        attachment: Attachment,
        output_dir: Optional[str] = None
    ) -> str:
        """
        Download attachment to local file.
        
        Args:
            attachment: Attachment to download
            output_dir: Optional output directory (uses temp dir if not provided)
            
        Returns:
            Path to downloaded file
        """
        try:
            # Determine output directory
            if output_dir is None:
                if self.temp_dir is None:
                    self.temp_dir = tempfile.mkdtemp(prefix='email_pricing_bot_')
                output_dir = self.temp_dir
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Download attachment data
            data = self.gmail_client.get_attachment(
                attachment.message_id,
                attachment.attachment_id
            )
            
            # Write to file
            output_path = os.path.join(output_dir, attachment.filename)
            with open(output_path, 'wb') as f:
                f.write(data)
            
            attachment.local_path = output_path
            
            logger.info(
                "Attachment downloaded",
                filename=attachment.filename,
                size=len(data),
                path=output_path
            )
            
            return output_path
            
        except Exception as e:
            logger.error(
                "Failed to download attachment",
                error=str(e),
                filename=attachment.filename
            )
            raise EmailError(f"Failed to download {attachment.filename}: {e}")
    
    def download_attachments(
        self,
        attachments: List[Attachment],
        output_dir: Optional[str] = None
    ) -> List[str]:
        """
        Download multiple attachments.
        
        Args:
            attachments: List of attachments to download
            output_dir: Optional output directory
            
        Returns:
            List of paths to downloaded files
        """
        paths = []
        
        for attachment in attachments:
            try:
                path = self.download_attachment(attachment, output_dir)
                paths.append(path)
            except EmailError as e:
                logger.warning(
                    "Skipping attachment due to download error",
                    filename=attachment.filename,
                    error=str(e)
                )
        
        return paths
    
    def cleanup(self):
        """Clean up temporary files."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
                logger.info(
                    "Temporary directory cleaned up",
                    path=self.temp_dir
                )
                self.temp_dir = None
            except Exception as e:
                logger.warning(
                    "Failed to clean up temporary directory",
                    error=str(e),
                    path=self.temp_dir
                )
    
    def __del__(self):
        """Cleanup on deletion."""
        self.cleanup()
