"""
Notification module.

Handles email notifications and report generation.
"""

from .email_sender import EmailSender
from .report_builder import ReportBuilder

__all__ = ['EmailSender', 'ReportBuilder']
