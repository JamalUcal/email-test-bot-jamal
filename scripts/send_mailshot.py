#!/usr/bin/env python3
"""
Mailshot utility for sending HTML emails to multiple recipients.

Sends HTML emails to a list of recipients using Gmail API with domain-wide delegation.
Logs successful and failed sends for clean re-runs.
"""

import sys
import os
import argparse
import json
import re
import base64
import mimetypes
from pathlib import Path
from typing import List, Set, Dict, Any, Tuple, Optional, cast
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gmail.gmail_client import GmailClient
from utils.logger import setup_logger

logger = setup_logger(__name__)


def read_addresses(file_path: Path) -> List[str]:
    """
    Read email addresses from CSV file.
    
    Args:
        file_path: Path to addresses CSV file
        
    Returns:
        List of email addresses (stripped, non-empty)
    """
    addresses: List[str] = []
    
    if not file_path.exists():
        logger.error(f"Addresses file not found: {file_path}")
        return addresses
    
    with open(file_path, 'r') as f:
        for line in f:
            address = line.strip()
            if address and '@' in address:
                addresses.append(address)
    
    logger.info(f"Loaded {len(addresses)} addresses from {file_path.name}")
    return addresses


def read_already_sent(file_path: Path) -> Set[str]:
    """
    Read already-sent email addresses from log file.
    
    Args:
        file_path: Path to success log file
        
    Returns:
        Set of email addresses that were already sent
    """
    already_sent: Set[str] = set()
    
    if not file_path.exists():
        return already_sent
    
    with open(file_path, 'r') as f:
        for line in f:
            address = line.strip()
            if address:
                already_sent.add(address)
    
    if already_sent:
        logger.info(f"Found {len(already_sent)} already-sent addresses in {file_path.name}")
    
    return already_sent


def append_to_log(file_path: Path, email: str) -> None:
    """
    Append email address to log file.
    
    Args:
        file_path: Path to log file
        email: Email address to append
    """
    # Create parent directory if it doesn't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(file_path, 'a') as f:
        f.write(f"{email}\n")


def read_subject(file_path: Path) -> str:
    """
    Read email subject from file.
    
    Args:
        file_path: Path to subject file
        
    Returns:
        Email subject (stripped)
    """
    if not file_path.exists():
        logger.error(f"Subject file not found: {file_path}")
        return "No Subject"
    
    with open(file_path, 'r') as f:
        subject = f.read().strip()
    
    logger.info(f"Subject: {subject}")
    return subject


def read_html_body(file_path: Path) -> str:
    """
    Read HTML email body from file.
    
    Args:
        file_path: Path to HTML file
        
    Returns:
        HTML content
    """
    if not file_path.exists():
        logger.error(f"HTML body file not found: {file_path}")
        return ""
    
    with open(file_path, 'r') as f:
        html_body = f.read()
    
    logger.info(f"Loaded HTML body ({len(html_body)} characters)")
    return html_body


def parse_image_references(html_content: str) -> List[str]:
    """
    Parse image references from HTML content.
    
    Finds all $imagename$ patterns in the HTML.
    
    Args:
        html_content: HTML content with $imagename$ references
        
    Returns:
        List of image filenames
    """
    pattern = r'\$([^$]+)\$'
    matches = re.findall(pattern, html_content)
    
    if matches:
        logger.info(f"Found {len(matches)} image reference(s): {matches}")
    
    return matches


def replace_image_references_with_cid(html_content: str, image_filenames: List[str]) -> str:
    """
    Replace $imagename$ with cid: references for embedded images.
    
    Args:
        html_content: HTML content with $imagename$ references
        image_filenames: List of image filenames to replace
        
    Returns:
        HTML content with cid: references
    """
    result = html_content
    for filename in image_filenames:
        # Replace $filename$ with cid:filename
        pattern = f'\\${re.escape(filename)}\\$'
        cid_ref = f'cid:{filename}'
        result = re.sub(pattern, cid_ref, result)
    
    return result


def load_image(image_path: Path) -> Tuple[bytes, str]:
    """
    Load image file and determine MIME type.
    
    Args:
        image_path: Path to image file
        
    Returns:
        Tuple of (image_data, mime_subtype)
        
    Raises:
        FileNotFoundError: If image file doesn't exist
    """
    if not image_path.exists():
        raise FileNotFoundError(f"Image not found: {image_path}")
    
    with open(image_path, 'rb') as f:
        image_data = f.read()
    
    # Determine MIME subtype from extension
    mime_type, _ = mimetypes.guess_type(str(image_path))
    if mime_type and mime_type.startswith('image/'):
        mime_subtype = mime_type.split('/')[1]
    else:
        # Default to png if we can't determine
        mime_subtype = 'png'
    
    logger.info(f"Loaded image: {image_path.name} ({len(image_data)} bytes, type: {mime_subtype})")
    
    return image_data, mime_subtype


def create_email_with_images(
    to: str,
    subject: str,
    html_body: str,
    from_email: str,
    from_name: str,
    reply_to: Optional[str],
    bounce_email: Optional[str],
    image_paths: Dict[str, Path]
) -> str:
    """
    Create a MIME multipart email with embedded images.
    
    Args:
        to: Recipient email address
        subject: Email subject
        html_body: HTML body with cid: references
        from_email: Sender email address (must be delegated user)
        from_name: Display name for sender
        reply_to: Reply-To address (where replies go), optional
        bounce_email: Return-Path for bounces, optional
        image_paths: Dict mapping filenames to their paths
        
    Returns:
        Base64-encoded raw email message
    """
    # Create multipart/related message
    msg = MIMEMultipart('related')
    msg['Subject'] = subject
    msg['From'] = f'"{from_name}" <{from_email}>'
    msg['To'] = to
    
    # Set Reply-To header if different from From
    if reply_to:
        msg['Reply-To'] = reply_to
    
    # Set Return-Path for bounces
    if bounce_email:
        msg['Return-Path'] = bounce_email
    
    # Add HTML body
    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)
    
    # Add embedded images
    for filename, image_path in image_paths.items():
        try:
            image_data, mime_subtype = load_image(image_path)
            
            # Create image part
            image_part = MIMEImage(image_data, _subtype=mime_subtype)
            image_part.add_header('Content-ID', f'<{filename}>')
            image_part.add_header('Content-Disposition', 'inline', filename=filename)
            
            msg.attach(image_part)
            
        except Exception as e:
            logger.error(f"Failed to attach image {filename}: {str(e)}")
            raise
    
    # Encode message
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    
    return raw


def send_mailshot(
    gmail_client: GmailClient,
    recipients: List[str],
    subject: str,
    html_body: str,
    from_email: str,
    from_name: str,
    reply_to: Optional[str],
    bounce_email: Optional[str],
    success_log: Path,
    fail_log: Path,
    img_folder: Path
) -> Dict[str, int]:
    """
    Send HTML emails to multiple recipients with embedded images.
    
    Args:
        gmail_client: Initialized GmailClient instance
        recipients: List of recipient email addresses
        subject: Email subject
        html_body: HTML email body (may contain $imagename$ references)
        from_email: Sender email address (must be delegated user)
        from_name: Display name for sender (e.g., UCAL Exports)
        reply_to: Reply-To address (replies go here), optional
        bounce_email: Return-Path for bounces, optional
        success_log: Path to success log file
        fail_log: Path to failure log file
        img_folder: Path to images folder
        
    Returns:
        Dictionary with counts: {'sent': int, 'failed': int}
    """
    sent_count = 0
    failed_count = 0
    
    # Parse image references from HTML
    image_filenames = parse_image_references(html_body)
    
    # Prepare image paths
    image_paths: Dict[str, Path] = {}
    if image_filenames:
        print(f"\nPreparing {len(image_filenames)} embedded image(s)...")
        for filename in image_filenames:
            image_path = img_folder / filename
            if not image_path.exists():
                logger.error(f"Image not found: {image_path}")
                print(f"  ✗ Image not found: {filename}")
                raise FileNotFoundError(f"Image not found: {filename}")
            image_paths[filename] = image_path
            print(f"  ✓ {filename}")
    
    # Replace $imagename$ with cid: references
    html_with_cids = replace_image_references_with_cid(html_body, image_filenames)
    
    print(f"\n{'━'*50}")
    print(f"Sending to {len(recipients)} recipient(s)...\n")
    
    for email in recipients:
        try:
            if image_paths:
                # Create multipart message with embedded images
                raw_message = create_email_with_images(
                    to=email,
                    subject=subject,
                    html_body=html_with_cids,
                    from_email=from_email,
                    from_name=from_name,
                    reply_to=reply_to,
                    bounce_email=bounce_email,
                    image_paths=image_paths
                )
                
                # Send via Gmail API using the authenticated service
                from googleapiclient.errors import HttpError
                
                if gmail_client.service is None:
                    raise Exception("Gmail service not initialized")
                
                try:
                    gmail_service = cast(Any, gmail_client.service)
                    result = cast(Dict[str, Any], gmail_service.users().messages().send(
                        userId='me',
                        body={'raw': raw_message}
                    ).execute())
                    
                    message_id = result['id']
                except HttpError as e:
                    # Log the full error for debugging
                    logger.error(f"Gmail API HttpError: {e.resp.status} - {e.error_details if hasattr(e, 'error_details') else str(e)}")
                    raise Exception(f"Gmail API error: {str(e)}")
            else:
                # No images, create simple HTML message with display name
                from email.mime.text import MIMEText as SimpleMIMEText
                
                msg = SimpleMIMEText(html_body, 'html')
                msg['Subject'] = subject
                msg['From'] = f'"{from_name}" <{from_email}>'
                msg['To'] = email
                
                # Set Reply-To header if different from From
                if reply_to:
                    msg['Reply-To'] = reply_to
                
                # Set Return-Path for bounces
                if bounce_email:
                    msg['Return-Path'] = bounce_email
                
                # Encode and send
                raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
                
                if gmail_client.service is None:
                    raise Exception("Gmail service not initialized")
                
                gmail_service = cast(Any, gmail_client.service)
                result = cast(Dict[str, Any], gmail_service.users().messages().send(
                    userId='me',
                    body={'raw': raw}
                ).execute())
                
                message_id = result['id']
            
            # Log success
            append_to_log(success_log, email)
            sent_count += 1
            
            # Truncate message ID for display
            msg_id_short = message_id[:12] + "..." if len(message_id) > 12 else message_id
            print(f"  ✓ {email} [msg_id: {msg_id_short}]")
            
            logger.info(f"Sent to {email}", message_id=message_id)
            
        except Exception as e:
            # Log failure
            append_to_log(fail_log, email)
            failed_count += 1
            
            error_msg = str(e)
            # Truncate error for display
            error_short = error_msg[:50] + "..." if len(error_msg) > 50 else error_msg
            print(f"  ✗ {email} [Error: {error_short}]")
            
            logger.error(f"Failed to send to {email}", error=error_msg)
    
    return {'sent': sent_count, 'failed': failed_count}


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Send HTML mailshot to multiple recipients",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send to addresses not yet in success log
  python scripts/send_mailshot.py

  # Force resend to all addresses (ignore success log)
  python scripts/send_mailshot.py --force

  # Specify custom paths
  python scripts/send_mailshot.py --addresses config/mail-shot/my-addresses.csv
        """
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force resend to all addresses (ignore success log)'
    )
    
    parser.add_argument(
        '--addresses',
        type=str,
        default='config/mail-shot/addresses.csv',
        help='Path to addresses CSV file (default: config/mail-shot/addresses.csv)'
    )
    
    parser.add_argument(
        '--subject-file',
        type=str,
        default='config/mail-shot/email-subject.md',
        help='Path to subject file (default: config/mail-shot/email-subject.md)'
    )
    
    parser.add_argument(
        '--html-file',
        type=str,
        default='config/mail-shot/email.html',
        help='Path to HTML body file (default: config/mail-shot/email.html)'
    )
    
    parser.add_argument(
        '--from-name',
        type=str,
        default='UCAL Exports',
        help='Display name for sender (default: UCAL Exports)'
    )
    
    parser.add_argument(
        '--reply-to',
        type=str,
        default='sales@ucalexports.com',
        help='Reply-To address for replies (default: sales@ucalexports.com)'
    )
    
    parser.add_argument(
        '--bounce-email',
        type=str,
        default='noreply@ucalexports.com',
        help='Return-Path address for bounces (default: noreply@ucalexports.com)'
    )
    
    parser.add_argument(
        '--delegated-user',
        type=str,
        default='automation@ucalexports.com',
        help='User to impersonate for sending (default: automation@ucalexports.com)'
    )
    
    parser.add_argument(
        '--test-auth',
        action='store_true',
        help='Test authentication by getting Gmail profile (don\'t send emails)'
    )
    
    args = parser.parse_args()
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    addresses_path = base_dir / args.addresses
    subject_path = base_dir / args.subject_file
    html_path = base_dir / args.html_file
    img_folder_path = base_dir / "config" / "mail-shot" / "img"
    success_log_path = base_dir / "config" / "mail-shot" / "success_email.txt"
    fail_log_path = base_dir / "config" / "mail-shot" / "fail_email.txt"
    
    # Print header
    print("\n" + "="*50)
    print("📧  MAILSHOT UTILITY")
    print("="*50)
    
    # Load service account credentials
    service_account_info: Dict[str, Any] = {}
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not credentials_path or not os.path.exists(credentials_path):
        logger.error(
            "No service account credentials found. "
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
        print("\n❌ ERROR: Set GOOGLE_APPLICATION_CREDENTIALS environment variable")
        sys.exit(1)
    
    logger.info(f"Loading service account from: {credentials_path}")
    with open(credentials_path) as f:
        service_account_info = json.load(f)
    
    # Initialize Gmail client
    logger.info(f"Initializing Gmail client (delegated user: {args.delegated_user})")
    
    try:
        gmail_client = GmailClient(
            service_account_info=service_account_info,
            delegated_user=args.delegated_user
        )
    except Exception as e:
        logger.error(f"Failed to initialize Gmail client: {str(e)}")
        print(f"\n❌ ERROR: Failed to initialize Gmail client")
        print(f"   {str(e)}")
        sys.exit(1)
    
    # Test authentication if requested
    if args.test_auth:
        print(f"\n{'='*50}")
        print("🔐 TESTING AUTHENTICATION")
        print(f"{'='*50}\n")
        print(f"Testing Gmail API access for: {args.delegated_user}")
        
        try:
            if gmail_client.service is None:
                raise Exception("Gmail service not initialized")
            
            gmail_service = cast(Any, gmail_client.service)
            profile = gmail_service.users().getProfile(userId='me').execute()
            
            print(f"\n✅ Authentication successful!")
            print(f"   Email: {profile.get('emailAddress')}")
            print(f"   Total messages: {profile.get('messagesTotal')}")
            print(f"   Total threads: {profile.get('threadsTotal')}")
            print(f"\nYou can now send emails. Remove --test-auth flag to proceed.\n")
            sys.exit(0)
            
        except Exception as e:
            print(f"\n❌ Authentication test FAILED")
            print(f"   Error: {str(e)}")
            print(f"\n📋 Troubleshooting steps:")
            print(f"   1. Verify domain-wide delegation is enabled in GCP Console:")
            print(f"      https://console.cloud.google.com/iam-admin/serviceaccounts")
            print(f"   2. Verify delegated user exists: {args.delegated_user}")
            print(f"   3. Check Google Workspace Admin > Security > API Controls")
            print(f"      > Domain-wide Delegation")
            print(f"   4. Ensure these scopes are authorized:")
            print(f"      - https://www.googleapis.com/auth/gmail.send")
            print(f"   5. Service account client ID: {service_account_info.get('client_id')}")
            print(f"\n")
            sys.exit(1)
    
    # Read email content
    subject = read_subject(subject_path)
    html_body = read_html_body(html_path)
    
    if not html_body:
        print("\n❌ ERROR: HTML body is empty")
        sys.exit(1)
    
    # Read addresses
    all_addresses = read_addresses(addresses_path)
    
    if not all_addresses:
        print(f"\n❌ ERROR: No valid addresses found in {addresses_path}")
        sys.exit(1)
    
    # Filter out already-sent addresses (unless --force)
    already_sent: Set[str] = set()
    if not args.force:
        already_sent = read_already_sent(success_log_path)
    
    recipients = [addr for addr in all_addresses if addr not in already_sent]
    skipped_count = len(all_addresses) - len(recipients)
    
    # Display summary
    print(f"\nSubject: {subject}")
    print(f"From: \"{args.from_name}\" <{args.delegated_user}>")
    print(f"Reply-To: {args.reply_to}")
    print(f"Bounce Address: {args.bounce_email}")
    print(f"\nRecipients:")
    print(f"  • Total: {len(all_addresses)}")
    print(f"  • To send: {len(recipients)}")
    print(f"  • Skipped (already sent): {skipped_count}")
    
    if not recipients:
        print("\n✅ All addresses already sent. Use --force to resend.")
        sys.exit(0)
    
    # Send emails
    results = send_mailshot(
        gmail_client=gmail_client,
        recipients=recipients,
        subject=subject,
        html_body=html_body,
        from_email=args.delegated_user,  # Gmail API requires From to be delegated user
        from_name=args.from_name,
        reply_to=args.reply_to,
        bounce_email=args.bounce_email,
        success_log=success_log_path,
        fail_log=fail_log_path,
        img_folder=img_folder_path
    )
    
    # Display summary
    print(f"\n{'━'*50}")
    print("Summary:")
    print(f"  • Sent: {results['sent']}")
    print(f"  • Failed: {results['failed']}")
    print(f"  • Skipped: {skipped_count} (already sent)")
    print(f"\nLogs:")
    print(f"  • Success: {success_log_path}")
    print(f"  • Failed: {fail_log_path}")
    print("="*50 + "\n")
    
    logger.info(
        "Mailshot complete",
        sent=results['sent'],
        failed=results['failed'],
        skipped=skipped_count
    )
    
    # Exit with error code if any failed
    if results['failed'] > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        print(f"\n❌ FATAL ERROR: {str(e)}")
        sys.exit(1)

