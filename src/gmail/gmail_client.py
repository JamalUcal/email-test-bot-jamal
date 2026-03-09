"""
Gmail API client with domain-wide delegation support.

Handles authentication and email retrieval from Gmail API.
"""

import base64
from typing import List, Dict, Optional, Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource
from datetime import datetime
from email.mime.text import MIMEText

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from utils.logger import get_logger
from utils.exceptions import EmailError, AuthenticationError

logger = get_logger(__name__)


class GmailClient:
    """Gmail API client with domain-wide delegation."""
    
    # Gmail API scopes
    SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.send'
    ]
    
    def __init__(self, service_account_info: Optional[Dict[str, Any]], delegated_user: str):
        """
        Initialize Gmail client.
        
        Args:
            service_account_info: Service account credentials JSON (optional - uses ADC if None)
            delegated_user: Email address to impersonate
        """
        self.delegated_user = delegated_user
        self.service: Optional['Resource'] = None
        
        try:
            # Create credentials with domain-wide delegation
            # NOTE: Domain-wide delegation REQUIRES a service account key with private key
            # Application Default Credentials (ADC) cannot be used because they don't
            # provide access to the private key needed for JWT signing
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=self.SCOPES
            )
            
            logger.info(
                "Service account credentials loaded",
                service_account_email=service_account_info.get('client_email', 'N/A') if service_account_info else 'N/A'
            )
            
            # Delegate to the specified user
            delegated_credentials = credentials.with_subject(delegated_user)
            
            # Build Gmail service
            self.service = build('gmail', 'v1', credentials=delegated_credentials)
            
            logger.info(
                "Gmail client initialized",
                delegated_user=delegated_user
            )
            
        except Exception as e:
            error_msg = str(e).lower()
            
            # Provide specific guidance for common delegation errors
            if "unauthorized_client" in error_msg or "client is unauthorized" in error_msg:
                logger.error(
                    "Domain-wide delegation failed - Check delegation configuration",
                    error=str(e),
                    delegated_user=delegated_user,
                    service_account_email=service_account_info.get('client_email', 'N/A') if service_account_info else 'N/A'
                )
                logger.error(
                    f"DELEGATION ERROR: Cannot delegate to user '{delegated_user}'. "
                    f"Common causes:\n"
                    f"  1. '{delegated_user}' is a GROUP, not a USER (delegation only works with real user accounts)\n"
                    f"  2. Domain-wide delegation not enabled on service account in GCP Console\n"
                    f"  3. Client ID not added to Google Workspace Admin (https://admin.google.com/ac/owl/domainwidedelegation)\n"
                    f"  4. Incorrect scopes configured in Workspace Admin\n"
                    f"  5. Propagation delay (wait 5-10 minutes after configuration changes)"
                )
            else:
                logger.error(
                    "Failed to initialize Gmail client",
                    error=str(e),
                    delegated_user=delegated_user
                )
            
            raise AuthenticationError(f"Gmail authentication failed for user '{delegated_user}': {e}")
    
    def list_messages(
        self,
        query: Optional[str] = None,
        max_results: int = 500,
        after_date: Optional[datetime] = None,
        before_date: Optional[datetime] = None
    ) -> List[Dict[str, str]]:
        """
        List messages matching query.
        
        Args:
            query: Gmail search query
            max_results: Maximum number of messages to return (default: 500 to fetch all in typical date ranges)
            after_date: Only return messages after this date
            before_date: Only return messages before this date (if None, defaults to 7 days after after_date)
            
        Returns:
            List of message metadata dicts with 'id' and 'threadId'
        """
        try:
            # Build query with date filter if provided
            full_query = query or ''
            if after_date:
                # Add after: filter
                after_str = after_date.strftime('%Y/%m/%d')
                date_query = f'after:{after_str}'
                
                # Add before: filter (use provided before_date or default to 7 days after start date)
                from datetime import timedelta
                effective_before_date = before_date if before_date else after_date + timedelta(days=7)
                before_str = effective_before_date.strftime('%Y/%m/%d')
                date_query = f'{date_query} before:{before_str}'
                
                full_query = f'{full_query} {date_query}' if full_query else date_query
                logger.info(
                    f"Added date filter to query: after:{after_str} before:{before_str}",
                    after_date=after_date.isoformat(),
                    before_date=effective_before_date.isoformat(),
                    date_filter=date_query
                )
            else:
                logger.warning("No after_date provided, fetching all messages")
            
            logger.info(
                f"Gmail API query: {full_query}",
                query=full_query,
                max_results=max_results
            )
            
            messages: List[Dict[str, str]] = []
            page_token = None
            
            while len(messages) < max_results:
                # Call Gmail API
                if self.service is None:
                    raise EmailError("Gmail service not initialized")
                
                # Cast to Any to access dynamic Gmail API methods
                gmail_service = cast(Any, self.service)
                result = gmail_service.users().messages().list(
                    userId='me',
                    q=full_query,
                    maxResults=min(max_results - len(messages), 500),
                    pageToken=page_token
                ).execute()
                    
                if 'messages' in result:
                    messages.extend(result['messages'])
                
                page_token = result.get('nextPageToken')
                if not page_token:
                    break
            
            logger.info(
                "Messages listed",
                count=len(messages)
            )
            
            return messages[:max_results]
            
        except HttpError as e:
            logger.error(
                "Failed to list messages",
                error=str(e),
                query=query
            )
            raise EmailError(f"Failed to list messages: {e}")
    
    def get_message(self, message_id: str) -> Dict[str, Any]:
        """
        Get full message details.
        
        Args:
            message_id: Gmail message ID
            
        Returns:
            Full message object with headers, body, and attachments
        """
        try:
            if self.service is None:
                raise EmailError("Gmail service not initialized")
            
            # Cast to Any to access dynamic Gmail API methods
            gmail_service = cast(Any, self.service)
            message = cast(Dict[str, Any], gmail_service.users().messages().get(
                userId='me',
                id=message_id,
                format='full'
            ).execute())
            
            logger.debug(
                "Message retrieved",
                message_id=message_id
            )
            
            return message
            
        except HttpError as e:
            logger.error(
                "Failed to get message",
                error=str(e),
                message_id=message_id
            )
            raise EmailError(f"Failed to get message {message_id}: {e}")
    
    def get_attachment(self, message_id: str, attachment_id: str) -> bytes:
        """
        Download attachment data.
        
        Args:
            message_id: Gmail message ID
            attachment_id: Attachment ID
            
        Returns:
            Attachment data as bytes
        """
        try:
            if self.service is None:
                raise EmailError("Gmail service not initialized")
            
            # Cast to Any to access dynamic Gmail API methods
            gmail_service = cast(Any, self.service)
            attachment = gmail_service.users().messages().attachments().get(
                userId='me',
                messageId=message_id,
                id=attachment_id
            ).execute()
            
            # Decode base64url encoded data
            data: bytes = base64.urlsafe_b64decode(attachment['data'])
            
            logger.debug(
                "Attachment downloaded",
                message_id=message_id,
                attachment_id=attachment_id,
                size=len(data)
            )
            
            return data
            
        except HttpError as e:
            logger.error(
                "Failed to download attachment",
                error=str(e),
                message_id=message_id,
                attachment_id=attachment_id
            )
            raise EmailError(f"Failed to download attachment: {e}")
    
    def send_message(
        self,
        to: List[str],
        subject: str,
        body: str,
        html_body: Optional[str] = None,
        from_email: Optional[str] = None
    ) -> str:
        """
        Send an email message.
        
        Args:
            to: List of recipient email addresses
            subject: Email subject
            body: Plain text body
            html_body: Optional HTML body
            from_email: Optional sender email (defaults to delegated user)
            
        Returns:
            Sent message ID
        """
        try:
            # Create message
            message = MIMEText(html_body or body, 'html' if html_body else 'plain')
            message['to'] = ', '.join(to)
            message['subject'] = subject
            
            # Set from address if provided (otherwise uses delegated user)
            if from_email:
                message['from'] = from_email
            
            # Encode message
            raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
            
            # Send message
            if self.service is None:
                raise EmailError("Gmail service not initialized")
            
            # Cast to Any to access dynamic Gmail API methods
            gmail_service = cast(Any, self.service)
            result = cast(Dict[str, Any], gmail_service.users().messages().send(
                userId='me',
                body={'raw': raw}
            ).execute())
            
            logger.info(
                "Email sent",
                message_id=result['id'],
                to=to,
                from_email=from_email or self.delegated_user,
                subject=subject
            )
            
            message_id: str = result['id']
            return message_id
            
        except HttpError as e:
            logger.error(
                "Failed to send email",
                error=str(e),
                to=to,
                subject=subject
            )
            raise EmailError(f"Failed to send email: {e}")
    
    @staticmethod
    def parse_headers(message: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract headers from message.
        
        Args:
            message: Gmail message object
            
        Returns:
            Dict of header name -> value
        """
        headers: Dict[str, str] = {}
        
        if 'payload' in message and 'headers' in message['payload']:
            for header in message['payload']['headers']:
                headers[header['name'].lower()] = header['value']
        
        return headers
    
    @staticmethod
    def get_header(message: Dict[str, Any], header_name: str) -> Optional[str]:
        """
        Get specific header value from message.
        
        Args:
            message: Gmail message object
            header_name: Header name (case-insensitive)
            
        Returns:
            Header value or None if not found
        """
        headers = GmailClient.parse_headers(message)
        return headers.get(header_name.lower())
    
    @staticmethod
    def get_message_date(message: Dict[str, Any]) -> Optional[datetime]:
        """
        Get message date as datetime.
        
        Args:
            message: Gmail message object
            
        Returns:
            Message date (timezone-aware UTC) or None if not found
        """
        if 'internalDate' in message:
            # internalDate is in milliseconds since epoch (UTC)
            timestamp = int(message['internalDate']) / 1000
            # Return timezone-aware datetime in UTC
            from datetime import timezone
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        return None
    
    @staticmethod
    def extract_email_address(email_string: str) -> str:
        """
        Extract email address from string like "Name <email@domain.com>".
        
        Args:
            email_string: Email string with optional name
            
        Returns:
            Email address only
        """
        if '<' in email_string and '>' in email_string:
            start = email_string.index('<') + 1
            end = email_string.index('>')
            return email_string[start:end].strip()
        
        return email_string.strip()
    
    @staticmethod
    def get_domain(email_address: str) -> str:
        """
        Extract domain from email address.
        
        Args:
            email_address: Email address
            
        Returns:
            Domain part of email address
        """
        email = GmailClient.extract_email_address(email_address)
        if '@' in email:
            return email.split('@')[1].lower()
        
        return ''
    
    def get_message_body(self, message: Dict[str, Any]) -> Optional[str]:
        """
        Extract plain text body from message.
        
        Args:
            message: Gmail message object
            
        Returns:
            Plain text body or None if not found
        """
        try:
            payload = message.get('payload', {})
            
            # Check if body is directly in payload
            if 'body' in payload and 'data' in payload['body']:
                body_data = payload['body']['data']
                return base64.urlsafe_b64decode(body_data).decode('utf-8')
            
            # Check parts for text/plain
            if 'parts' in payload:
                for part in payload['parts']:
                    if part.get('mimeType') == 'text/plain':
                        if 'data' in part.get('body', {}):
                            body_data = part['body']['data']
                            return base64.urlsafe_b64decode(body_data).decode('utf-8')
                    
                    # Check nested parts (multipart/alternative, multipart/related, etc.)
                    if 'parts' in part:
                        for subpart in part['parts']:
                            if subpart.get('mimeType') == 'text/plain':
                                if 'data' in subpart.get('body', {}):
                                    body_data = subpart['body']['data']
                                    return base64.urlsafe_b64decode(body_data).decode('utf-8')
                            
                            # Check even deeper nested parts (e.g., multipart/alternative inside multipart/related)
                            if 'parts' in subpart:
                                for deeppart in subpart['parts']:
                                    if deeppart.get('mimeType') == 'text/plain':
                                        if 'data' in deeppart.get('body', {}):
                                            body_data = deeppart['body']['data']
                                            return base64.urlsafe_b64decode(body_data).decode('utf-8')
            
            logger.debug("No plain text body found in message")
            return None
            
        except Exception as e:
            logger.warning(
                "Failed to extract message body",
                error=str(e),
                message_id=message.get('id')
            )
            return None
    
    @staticmethod
    def parse_supplier_tag(body: Optional[str]) -> Optional[str]:
        """
        Extract supplier name from SUPPLIER: tag in email body.
        
        Args:
            body: Email body text
            
        Returns:
            Supplier name or None if not found
        """
        if not body:
            return None
        
        import re
        
        # Look for SUPPLIER: tag (case-insensitive)
        # Pattern: SUPPLIER: <supplier_name>
        # Can be followed by newline or end of string
        pattern = r'SUPPLIER:\s*([A-Z0-9_\-]+)'
        match = re.search(pattern, body, re.IGNORECASE | re.MULTILINE)
        
        if match:
            supplier_name = match.group(1).strip().upper()
            logger.info(
                f"Found SUPPLIER tag in email body: {supplier_name}",
                supplier_name=supplier_name
            )
            return supplier_name
        
        return None
    
    @staticmethod
    def parse_forwarded_email(body: Optional[str]) -> Optional[str]:
        """
        Extract original sender email from forwarded email body.
        
        Supports Gmail and Outlook forward formats:
        - Gmail: "---------- Forwarded message ---------" followed by "From: email@domain.com"
        - Outlook: "From:" followed by email address
        
        Also handles Google Groups "via" format where the actual sender is in CC/To:
        - From: 'Name' via GroupName <group@domain.com>
        - In this case, checks CC and To fields for the actual external sender
        
        Handles nested forwards: if the first forwarded block contains only internal
        addresses (@ucalexports.com), checks a second forwarded block if present.
        Max depth: 2 forwarded blocks.
        
        Args:
            body: Email body text
            
        Returns:
            Original sender email address or None if not found
        """
        if not body:
            return None
        
        import re
        from typing import Tuple
        
        email_pattern = r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        
        def is_internal_email(email: str) -> bool:
            """Check if email is from internal domain."""
            return email.lower().endswith('@ucalexports.com')
        
        def extract_forwarded_header_block(body_text: str, start_pos: int = 0) -> Tuple[Optional[str], int]:
            """Extract the header block from a forwarded message.
            
            Captures everything after the forwarded message marker until a blank line,
            which handles wrapped headers that don't follow RFC 2822 folding rules.
            
            Args:
                body_text: The email body text to search
                start_pos: Position in body_text to start searching from
                
            Returns:
                Tuple of (header_block content or None, end position of match)
            """
            # Gmail forward pattern - capture everything after marker until blank line (end of headers)
            # This handles non-standard line wrapping where continuation doesn't start with whitespace
            gmail_pattern = r'-{5,}\s*Forwarded message\s*-{5,}\s*\r?\n(.*?)(?:\r?\n\r?\n|\n\n)'
            gmail_match = re.search(gmail_pattern, body_text[start_pos:], re.IGNORECASE | re.DOTALL)
            if gmail_match:
                # Return header block and absolute end position
                return gmail_match.group(1), start_pos + gmail_match.end()
            return None, start_pos
        
        def extract_emails_from_header_line(header_name: str, header_block: str) -> list:
            """Extract all email addresses from a specific header line.
            
            Handles wrapped lines that don't follow RFC 2822 folding rules.
            Captures from header until the next header line or end of block.
            """
            # Capture from header name until next header keyword or end of string
            # Use \Z for end of string (not $, which matches end of line with MULTILINE)
            pattern = rf'^{header_name}:\s*(.+?)(?=\r?\n(?:From|Date|Subject|To|Cc):|\Z)'
            match = re.search(pattern, header_block, re.IGNORECASE | re.MULTILINE | re.DOTALL)
            if match:
                line_content = match.group(1)
                return re.findall(email_pattern, line_content)
            return []
        
        def check_header_block_for_external(header_block: str, block_number: int) -> Optional[str]:
            """Check a header block for external email addresses.
            
            Args:
                header_block: The extracted header block content
                block_number: Which forwarded block this is (1 or 2) for logging
                
            Returns:
                External email address if found, None otherwise
            """
            # Extract emails from From, Cc, and To headers
            from_emails = extract_emails_from_header_line('From', header_block)
            cc_emails = extract_emails_from_header_line('Cc', header_block)
            to_emails = extract_emails_from_header_line('To', header_block)
            
            # Check if From line contains "via" pattern (Google Groups)
            from_line_match = re.search(r'^From:\s*(.+?)(?:\r?\n|$)', header_block, re.IGNORECASE | re.MULTILINE)
            has_via_pattern = from_line_match and ' via ' in from_line_match.group(1)
            
            block_label = "nested forward" if block_number == 2 else "Gmail format"
            
            # If From email is external and not a "via" pattern, use it
            if from_emails:
                from_email = from_emails[0]
                if not is_internal_email(from_email) and not has_via_pattern:
                    logger.info(
                        f"Found forwarded email ({block_label}, From): {from_email}",
                        email_address=from_email,
                        forward_depth=block_number
                    )
                    return from_email
            
            # From is internal or "via" pattern - check CC and To for external emails
            # Priority: CC first (more likely to have the actual sender), then To
            all_candidate_emails = cc_emails + to_emails
            
            for email in all_candidate_emails:
                if not is_internal_email(email):
                    logger.info(
                        f"Found forwarded email ({block_label}, CC/To fallback): {email}",
                        email_address=email,
                        reason="From was internal or via pattern",
                        forward_depth=block_number
                    )
                    return email
            
            # All addresses in this block are internal
            return None
        
        def all_addresses_internal(header_block: str) -> bool:
            """Check if all addresses in a header block are internal."""
            from_emails = extract_emails_from_header_line('From', header_block)
            cc_emails = extract_emails_from_header_line('Cc', header_block)
            to_emails = extract_emails_from_header_line('To', header_block)
            
            all_emails = from_emails + cc_emails + to_emails
            return all(is_internal_email(email) for email in all_emails) if all_emails else True
        
        # Try Gmail forward format - check first forwarded block
        first_header_block, first_end_pos = extract_forwarded_header_block(body, 0)
        
        if first_header_block:
            # Check first block for external addresses
            external_email = check_header_block_for_external(first_header_block, block_number=1)
            if external_email:
                return external_email
            
            # First block is all internal - check for a second forwarded block (max depth: 2)
            if all_addresses_internal(first_header_block):
                second_header_block, _ = extract_forwarded_header_block(body, first_end_pos)
                
                if second_header_block:
                    logger.info(
                        "First forwarded block is all internal, checking nested forward",
                        first_block_end_pos=first_end_pos
                    )
                    external_email = check_header_block_for_external(second_header_block, block_number=2)
                    if external_email:
                        return external_email
            
            # No external found in either block - return first block's From if available
            from_emails = extract_emails_from_header_line('From', first_header_block)
            if from_emails:
                logger.info(
                    f"Found forwarded email (Gmail format, internal): {from_emails[0]}",
                    email_address=from_emails[0]
                )
                return from_emails[0]
        
        # Outlook/generic forward pattern fallback
        # Look for "From:" at start of line (case-insensitive)
        outlook_pattern = r'^From:\s*(.+?)(?:\r?\n|$)'
        for line in body.split('\n'):
            outlook_match = re.search(outlook_pattern, line, re.IGNORECASE)
            if outlook_match:
                from_line = outlook_match.group(1).strip()
                # Extract email from the line
                email_match = re.search(email_pattern, from_line)
                if email_match:
                    email_address = email_match.group(1)
                    # Skip if it's from our own domain (internal forward, not original)
                    if not email_address.lower().endswith('@ucalexports.com'):
                        logger.info(
                            f"Found forwarded email (Outlook format): {email_address}",
                            email_address=email_address
                        )
                        return email_address
        
        return None
    
    @staticmethod
    def parse_forwarded_subject(body: Optional[str]) -> Optional[str]:
        """
        Extract original subject line from forwarded email body.
        
        Supports Gmail and Outlook forward formats:
        - Gmail: "---------- Forwarded message ---------" followed by "Subject:"
        - Outlook: "Subject:" at start of line
        
        Args:
            body: Email body text
            
        Returns:
            Original subject line or None if not found
        """
        if not body:
            return None
        
        import re
        
        # Gmail forward pattern
        # Look for "---------- Forwarded message ---------" followed by "Subject:"
        gmail_pattern = r'-{5,}\s*Forwarded message\s*-{5,}.*?Subject:\s*(.+?)(?:\r?\n|$)'
        gmail_match = re.search(gmail_pattern, body, re.IGNORECASE | re.DOTALL)
        
        if gmail_match:
            subject_line = gmail_match.group(1).strip()
            logger.info(
                f"Found forwarded subject (Gmail format): {subject_line}",
                subject=subject_line
            )
            return subject_line
        
        # Outlook/generic forward pattern
        # Look for "Subject:" at start of line (case-insensitive)
        outlook_pattern = r'^Subject:\s*(.+?)(?:\r?\n|$)'
        for line in body.split('\n'):
            outlook_match = re.search(outlook_pattern, line, re.IGNORECASE)
            if outlook_match:
                subject_line = outlook_match.group(1).strip()
                logger.info(
                    f"Found forwarded subject (Outlook format): {subject_line}",
                    subject=subject_line
                )
                return subject_line
        
        return None