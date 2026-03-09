"""
Email processing module.

Handles Gmail API integration, email filtering, and attachment processing.
"""

from .gmail_client import GmailClient
from .email_processor import EmailProcessor
from .attachment_handler import AttachmentHandler

__all__ = ['GmailClient', 'EmailProcessor', 'AttachmentHandler']
