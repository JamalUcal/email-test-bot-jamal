"""
Email sender for summary notifications.

Sends formatted summary emails via Gmail API.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from orchestrator import EmailProcessingResult, FileOutput
    from gmail.email_processor import EmailResult

from gmail.gmail_client import GmailClient
from gmail.email_processor import ProcessingResults
from .report_builder import ReportBuilder
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FileEntry:
    """
    Represents a single file entry for summary reporting.
    
    This allows splitting emails with mixed results (some files succeeded, 
    some failed) into separate entries for the success/warning sections.
    """
    supplier_name: str
    subject: str
    email_date: Optional['datetime'] = None
    filename: str = ""
    drive_link: Optional[str] = None
    total_rows: int = 0
    valid_rows: int = 0
    error: Optional[str] = None
    is_success: bool = False
    # Reference to original email for additional context if needed
    message_id: Optional[str] = None
    from_address: Optional[str] = None
    is_scraper: bool = False
    scraper_url: Optional[str] = None
    # BigQuery reconciliation stats
    reconciliation_stats: Optional[dict] = None


class EmailSender:
    """Sends summary email notifications."""
    
    def __init__(self, gmail_client: GmailClient):
        """
        Initialize email sender.
        
        Args:
            gmail_client: GmailClient instance
        """
        self.gmail_client = gmail_client
        self.report_builder = ReportBuilder()
    
    @staticmethod
    def _split_results_into_file_entries(
        results: 'List[EmailProcessingResult]'
    ) -> Tuple[List[FileEntry], List[FileEntry], List['EmailProcessingResult'], List['EmailProcessingResult'], List['EmailProcessingResult']]:
        """
        Split email results into file-level entries for better categorization.
        
        Emails with mixed results (some files succeeded, some failed) are split
        so successful files appear in the success section and failed files in warnings.
        
        Args:
            results: List of EmailProcessingResult from orchestrator
            
        Returns:
            Tuple of:
            - success_files: List of FileEntry for successfully processed files
            - warning_files: List of FileEntry for files with errors/warnings
            - failed_emails: List of EmailProcessingResult for completely failed emails
            - skipped_emails: List of EmailProcessingResult for skipped emails
            - unknown_emails: List of EmailProcessingResult for unknown suppliers
        """
        success_files: List[FileEntry] = []
        warning_files: List[FileEntry] = []
        failed_emails: List['EmailProcessingResult'] = []
        skipped_emails: List['EmailProcessingResult'] = []
        unknown_emails: List['EmailProcessingResult'] = []
        
        for result in results:
            email = result.email_result
            is_scraper = email.from_address.startswith('scraper@')
            
            # Unknown suppliers go to their own section
            if email.is_unknown_domain:
                unknown_emails.append(result)
                continue
            
            # Emails with top-level errors (not file-level) go to failed
            if result.errors:
                failed_emails.append(result)
                continue
            
            # Emails where supplier was not detected go to warnings
            if email.is_ignored and not result.files_generated:
                entry = FileEntry(
                    supplier_name=email.supplier_name or 'UNKNOWN',
                    subject=email.subject,
                    email_date=email.date,
                    filename="(supplier not detected)",
                    error="Supplier not detected - add to supplier_config.json",
                    message_id=email.message_id,
                    from_address=email.from_address,
                    is_scraper=is_scraper,
                )
                warning_files.append(entry)
                continue
            
            # Skipped emails (no attachments, no files generated, no errors)
            if not result.files_generated and not email.attachments:
                skipped_emails.append(result)
                continue
            
            # Process file-level results
            if result.files_generated:
                for file_output in result.files_generated:
                    entry = FileEntry(
                        supplier_name=email.supplier_name or 'UNKNOWN',
                        subject=email.subject,
                        email_date=email.date,
                        filename=file_output.filename,
                        drive_link=file_output.drive_link,
                        total_rows=file_output.total_rows,
                        valid_rows=file_output.valid_rows,
                        error=file_output.error,
                        message_id=email.message_id,
                        from_address=email.from_address,
                        is_scraper=is_scraper,
                        scraper_url=email.scraper_url if hasattr(email, 'scraper_url') else None,
                        reconciliation_stats=file_output.reconciliation_stats if hasattr(file_output, 'reconciliation_stats') else None,
                    )
                    
                    # File is successful if it has a drive_file_id and no error
                    if file_output.drive_file_id and not file_output.error:
                        entry.is_success = True
                        success_files.append(entry)
                    else:
                        entry.is_success = False
                        warning_files.append(entry)
            else:
                # No files generated but has attachments - goes to warnings
                # (unsupported format, etc.)
                if email.attachments:
                    entry = FileEntry(
                        supplier_name=email.supplier_name or 'UNKNOWN',
                        subject=email.subject,
                        email_date=email.date,
                        filename="(no supported files)",
                        error="No supported file formats found",
                        message_id=email.message_id,
                        from_address=email.from_address,
                        is_scraper=is_scraper,
                    )
                    warning_files.append(entry)
        
        return success_files, warning_files, failed_emails, skipped_emails, unknown_emails
    
    def send_summary(
        self,
        results: ProcessingResults,
        recipients: List[str],
        dry_run: bool = False,
        from_email: Optional[str] = None
    ) -> bool:
        """
        Send summary email.
        
        Args:
            results: Processing results
            recipients: List of recipient email addresses
            dry_run: If True, log email but don't send
            from_email: Optional sender email address (e.g., pricing@ucalexports.com)
            
        Returns:
            True if sent successfully
        """
        try:
            # Build subject
            subject = f"Email Pricing Bot Summary - {results.emails_processed} emails processed"
            
            # Build reports
            text_body = self.report_builder.build_text_report(results)
            html_body = self.report_builder.build_html_report(results)
            
            if dry_run:
                logger.info(
                    "DRY RUN: Would send summary email",
                    recipients=recipients,
                    from_email=from_email,
                    subject=subject,
                    body_preview=text_body[:200]
                )
                return True
            
            # Send email
            message_id = self.gmail_client.send_message(
                to=recipients,
                subject=subject,
                body=text_body,
                html_body=html_body,
                from_email=from_email
            )
            
            logger.info(
                "Summary email sent",
                message_id=message_id,
                recipients=recipients
            )
            
            return True
            
        except Exception as e:
            import traceback
            logger.error(
                "Failed to send summary email (send_summary)",
                error=str(e),
                recipients=recipients
            )
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def build_summary_text_from_orchestrator_results(
        self,
        results: 'List[EmailProcessingResult]'
    ) -> tuple[str, str]:
        """
        Build summary email text from orchestrator results.
        
        Uses compact format and reordered sections:
        FAILED -> WARNINGS -> UNKNOWN -> SKIPPED -> ACTION ITEMS -> SUCCESSFUL
        
        Args:
            results: List of EmailProcessingResult from orchestrator
            
        Returns:
            Tuple of (subject, text_body)
        """
        # Separate email processing from web scraping results
        email_results = [r for r in results if not r.email_result.from_address.startswith('scraper@')]
        scraper_results = [r for r in results if r.email_result.from_address.startswith('scraper@')]
        
        # Calculate summary statistics - Email Processing
        emails_processed = len(email_results)
        email_files_generated = sum(len(r.files_generated) for r in email_results)
        email_errors = sum(len(r.errors) for r in email_results)
        email_warnings = sum(len(r.warnings) for r in email_results)
        email_file_errors = sum(1 for r in email_results for f in r.files_generated if f.error and 'upload' in f.error.lower())
        email_warnings += email_file_errors
        email_uploads = sum(1 for r in email_results for f in r.files_generated if f.drive_file_id)
        
        # Calculate summary statistics - Web Scraping
        scrapers_processed = len(scraper_results)
        scraper_files_generated = sum(len(r.files_generated) for r in scraper_results)
        scraper_errors = sum(len(r.errors) for r in scraper_results)
        scraper_warnings = sum(len(r.warnings) for r in scraper_results)
        scraper_file_errors = sum(1 for r in scraper_results for f in r.files_generated if f.error and 'upload' in f.error.lower())
        scraper_warnings += scraper_file_errors
        scraper_uploads = sum(1 for r in scraper_results for f in r.files_generated if f.drive_file_id)
        
        # Total statistics
        total_processed = len(results)
        total_errors = email_errors + scraper_errors
        total_warnings = email_warnings + scraper_warnings
        successful_uploads = email_uploads + scraper_uploads
        
        # Calculate parsing statistics
        total_rows_all_files = sum(f.total_rows for r in results for f in r.files_generated)
        total_valid_rows_all_files = sum(f.valid_rows for r in results for f in r.files_generated)
        total_parsing_errors = sum(f.parsing_errors_count for r in results for f in r.files_generated)
        
        # Calculate reconciliation statistics (BigQuery supersession)
        files_with_reconciliation = [
            f for r in results for f in r.files_generated 
            if hasattr(f, 'reconciliation_stats') and f.reconciliation_stats
        ]
        has_reconciliation = len(files_with_reconciliation) > 0
        total_duplicates_found = sum(
            (f.reconciliation_stats or {}).get('duplicates_found', 0) 
            for f in files_with_reconciliation
        )
        total_duplicates_removed = sum(
            (f.reconciliation_stats or {}).get('duplicates_removed', 0) 
            for f in files_with_reconciliation
        )
        total_synthetic_items = sum(
            (f.reconciliation_stats or {}).get('synthetic_items_added', 0) 
            for f in files_with_reconciliation
        )
        total_reconciliation_errors = sum(
            (f.reconciliation_stats or {}).get('items_with_errors', 0) 
            for f in files_with_reconciliation
        )
        
        # Split results into file-level entries for proper categorization
        success_files, warning_files, failed_emails, skipped_emails, unknown_emails = \
            self._split_results_into_file_entries(results)
        
        # Determine overall status
        if failed_emails or warning_files:
            if success_files:
                status = "⚠️ Partial Success" if failed_emails else "⚠️ Success with Warnings"
            else:
                status = "❌ Failed"
        else:
            status = "✅ Success"
        
        # Build summary
        subject = f"Pricing Bot - {status} - {total_processed} processed, {successful_uploads} files uploaded"
        
        # Header (with optional reconciliation section)
        reconciliation_section = ""
        if has_reconciliation:
            reconciliation_section = f"""
🔄 SUPERSESSION RECONCILIATION
   • Files Reconciled: {len(files_with_reconciliation)}
   • Duplicates Found: {total_duplicates_found:,} ({total_duplicates_removed:,} removed)
   • Synthetic Rows: {total_synthetic_items:,} (missing supersessions added)
   • Reconciliation Errors: {total_reconciliation_errors:,}
"""
        
        text_body: str = f"""📊 PRICING BOT SUMMARY
{'━'*20}

Execution Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
Status: {status}

📧 EMAIL PROCESSING
   • Emails: {emails_processed} processed
   • Files: {email_files_generated} generated, {email_uploads} uploaded
   • Errors: {email_errors} | Warnings: {email_warnings}

🌐 WEB SCRAPING
   • Suppliers: {scrapers_processed} processed
   • Files: {scraper_files_generated} generated, {scraper_uploads} uploaded
   • Errors: {scraper_errors} | Warnings: {scraper_warnings}

📊 PARSING SUMMARY
   • Total Rows: {total_rows_all_files:,}
   • Valid Rows: {total_valid_rows_all_files:,}
   • Parse Errors: {total_parsing_errors:,}
{reconciliation_section}
"""
        
        # Section 1: FAILED (emails with top-level errors)
        if failed_emails:
            text_body += f"\n❌ FAILED ({len(failed_emails)})\n"
            text_body += "─" * 20 + "\n\n"
            
            for idx, result in enumerate(failed_emails, 1):
                email = result.email_result
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                text_body += f"{idx}. {email.supplier_name or 'UNKNOWN'} - {date_str} - {email.subject[:50]}\n"
                
                text_body += "   Errors:\n"
                for error in result.errors:
                    text_body += f"     • {error}\n"
                
                text_body += "\n"
        
        # Section 2: WARNINGS (files with errors)
        if warning_files:
            text_body += f"\n⚠️ WARNINGS ({len(warning_files)})\n"
            text_body += "─" * 20 + "\n\n"
            
            for idx, entry in enumerate(warning_files, 1):
                date_str = entry.email_date.strftime('%d-%b-%y') if entry.email_date else 'Unknown'
                text_body += f"{idx}. {entry.supplier_name} - {date_str} - {entry.subject[:50]}\n"
                text_body += f"   File: {entry.filename} ❌\n"
                if entry.error:
                    text_body += f"   Error: {entry.error}\n"
                
                text_body += "\n"
        
        # Section 3: UNKNOWN SUPPLIERS
        if unknown_emails:
            text_body += f"\n❓ UNKNOWN SUPPLIERS ({len(unknown_emails)})\n"
            text_body += "─" * 20 + "\n\n"
            
            for idx, result in enumerate(unknown_emails, 1):
                email = result.email_result
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                text_body += f"{idx}. {email.from_address} - {date_str} - {email.subject[:50]}\n"
                text_body += f"   Domain: {email.from_domain}\n"
                text_body += "   Unable to process - no supplier detected. Resend with SUPPLIER: GTAUTO or SUPPLIER: TECHNOPARTS in the email body (as appropriate).\n"
                text_body += "   → Add to supplier_config.json if legitimate\n"
                text_body += "\n"
        
        # Section 4: SKIPPED
        if skipped_emails:
            text_body += f"\n⏭️ SKIPPED ({len(skipped_emails)})\n"
            text_body += "─" * 20 + "\n\n"
            
            for idx, result in enumerate(skipped_emails, 1):
                email = result.email_result
                is_scraper = email.from_address.startswith('scraper@')
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                text_body += f"{idx}. {email.supplier_name or 'UNKNOWN'} - {date_str} - {email.subject[:50]}\n"
                
                # Determine skip reason
                if is_scraper and hasattr(email, 'scraper_all_duplicates') and email.scraper_all_duplicates:
                    text_body += "   Reason: No new files (all duplicates)\n"
                elif not email.attachments:
                    text_body += "   Reason: No attachments\n"
                
                text_body += "\n"
        
        # Section 5: ACTION ITEMS
        action_items: List[str] = []
        
        if failed_emails:
            action_items.append(f"⚠️ Review {len(failed_emails)} failed email(s) - check errors above")
        
        if warning_files:
            action_items.append(f"⚠️ Review {len(warning_files)} file(s) with warnings")
        
        if unknown_emails:
            action_items.append(f"⚠️ Review {len(unknown_emails)} unknown supplier(s) - add to config if needed")
        
        expired_count = sum(1 for r in results if r.expiry_is_past)
        if expired_count:
            action_items.append(f"⚠️ {expired_count} file(s) with past expiry dates - verify with suppliers")
        
        fallback_count = sum(1 for r in results if r.brand_fallback_used)
        if fallback_count:
            action_items.append(f"ℹ️ {fallback_count} file(s) used fallback brand - consider adding keywords")
        
        if action_items:
            text_body += "\n📋 ACTION ITEMS\n"
            text_body += "─" * 20 + "\n\n"
            for item in action_items:
                text_body += f"{item}\n"
            text_body += "\n"
        
        # Section 6: SUCCESSFUL (at the end, compact format)
        if success_files:
            text_body += f"\n✅ SUCCESSFUL ({len(success_files)} files)\n"
            text_body += "─" * 20 + "\n\n"
            
            for idx, entry in enumerate(success_files, 1):
                date_str = entry.email_date.strftime('%d-%b-%y') if entry.email_date else 'Unknown'
                text_body += f"{idx}. {entry.supplier_name} - {date_str} - {entry.subject[:50]}\n"
                if entry.drive_link:
                    text_body += f"   File: <a href=\"{entry.drive_link}\">{entry.filename}</a>\n"
                else:
                    text_body += f"   File: {entry.filename}\n"
                if entry.total_rows > 0:
                    text_body += f"   Rows: {entry.valid_rows:,}/{entry.total_rows:,} valid\n"
                text_body += "\n"
        else:
            text_body += "\n✅ NO ACTION REQUIRED\n"
            text_body += "─" * 20 + "\n"
            text_body += "All emails processed successfully!\n\n"
        
        # Footer
        text_body += "─" * 20 + "\n"
        text_body += "Email Pricing Bot v1.0.0\n"
        if results and results[-1].email_result.date:
            last_processed = results[-1].email_result.date.strftime('%Y-%m-%d %H:%M:%S UTC')
            text_body += f"Last Processed: {last_processed}\n"
        text_body += "─" * 20 + "\n"
        
        return subject, text_body
    
    def build_html_summary_from_orchestrator_results(
        self,
        results: 'List[EmailProcessingResult]'
    ) -> str:
        """
        Build HTML summary email from orchestrator results.
        
        Uses compact format with clickable links and reordered sections:
        FAILED -> WARNINGS -> UNKNOWN -> SKIPPED -> ACTION ITEMS -> SUCCESSFUL
        
        Args:
            results: List of EmailProcessingResult from orchestrator
            
        Returns:
            HTML string
        """
        # Separate email processing from web scraping results
        email_results = [r for r in results if not r.email_result.from_address.startswith('scraper@')]
        scraper_results = [r for r in results if r.email_result.from_address.startswith('scraper@')]
        
        # Calculate summary statistics - Email Processing
        emails_processed = len(email_results)
        email_files_generated = sum(len(r.files_generated) for r in email_results)
        email_errors = sum(len(r.errors) for r in email_results)
        email_warnings = sum(len(r.warnings) for r in email_results)
        email_file_errors = sum(1 for r in email_results for f in r.files_generated if f.error and 'upload' in f.error.lower())
        email_warnings += email_file_errors
        email_uploads = sum(1 for r in email_results for f in r.files_generated if f.drive_file_id)
        
        # Calculate summary statistics - Web Scraping
        scrapers_processed = len(scraper_results)
        scraper_files_generated = sum(len(r.files_generated) for r in scraper_results)
        scraper_errors = sum(len(r.errors) for r in scraper_results)
        scraper_warnings = sum(len(r.warnings) for r in scraper_results)
        scraper_file_errors = sum(1 for r in scraper_results for f in r.files_generated if f.error and 'upload' in f.error.lower())
        scraper_warnings += scraper_file_errors
        scraper_uploads = sum(1 for r in scraper_results for f in r.files_generated if f.drive_file_id)
        
        # Total statistics
        total_processed = len(results)
        successful_uploads = email_uploads + scraper_uploads
        
        # Calculate parsing statistics
        total_rows_all_files = sum(f.total_rows for r in results for f in r.files_generated)
        total_valid_rows_all_files = sum(f.valid_rows for r in results for f in r.files_generated)
        total_parsing_errors = sum(f.parsing_errors_count for r in results for f in r.files_generated)
        
        # Calculate reconciliation statistics (BigQuery supersession)
        files_with_reconciliation = [
            f for r in results for f in r.files_generated 
            if hasattr(f, 'reconciliation_stats') and f.reconciliation_stats
        ]
        has_reconciliation = len(files_with_reconciliation) > 0
        total_duplicates_found = sum(
            (f.reconciliation_stats or {}).get('duplicates_found', 0) 
            for f in files_with_reconciliation
        )
        total_duplicates_removed = sum(
            (f.reconciliation_stats or {}).get('duplicates_removed', 0) 
            for f in files_with_reconciliation
        )
        total_synthetic_items = sum(
            (f.reconciliation_stats or {}).get('synthetic_items_added', 0) 
            for f in files_with_reconciliation
        )
        total_reconciliation_errors = sum(
            (f.reconciliation_stats or {}).get('items_with_errors', 0) 
            for f in files_with_reconciliation
        )
        
        # Split results into file-level entries for proper categorization
        success_files, warning_files, failed_emails, skipped_emails, unknown_emails = \
            self._split_results_into_file_entries(results)
        
        # Determine overall status
        if failed_emails or warning_files:
            if success_files:
                status = "⚠️ Partial Success" if failed_emails else "⚠️ Success with Warnings"
            else:
                status = "❌ Failed"
        else:
            status = "✅ Success"
        
        # Build HTML
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html>")
        html.append("<head>")
        html.append("<meta charset='utf-8'>")
        html.append("<style>")
        html.append("""
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; 
                   margin: 0; padding: 20px; background-color: #f5f5f5; color: #333; }
            .container { max-width: 800px; margin: 0 auto; background: white; border-radius: 8px; 
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow: hidden; }
            .header { background: linear-gradient(135deg, #2196F3, #1976D2); color: white; 
                     padding: 20px 25px; }
            .header h1 { margin: 0 0 5px 0; font-size: 22px; font-weight: 600; }
            .header .status { font-size: 14px; opacity: 0.9; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                         gap: 15px; padding: 20px 25px; background: #fafafa; border-bottom: 1px solid #eee; }
            .stat-box { background: white; padding: 15px; border-radius: 6px; border: 1px solid #e0e0e0; }
            .stat-box h3 { margin: 0 0 10px 0; font-size: 13px; color: #666; text-transform: uppercase; 
                          letter-spacing: 0.5px; }
            .stat-box .value { font-size: 24px; font-weight: 600; color: #1976D2; }
            .stat-box .detail { font-size: 12px; color: #888; margin-top: 5px; }
            .section { padding: 20px 25px; border-bottom: 1px solid #eee; }
            .section:last-child { border-bottom: none; }
            .section h2 { margin: 0 0 15px 0; font-size: 16px; font-weight: 600; display: flex; 
                         align-items: center; gap: 8px; }
            .section h2 .count { background: #e3f2fd; color: #1976D2; padding: 2px 8px; 
                                border-radius: 12px; font-size: 12px; }
            .section.failed h2 .count { background: #ffebee; color: #c62828; }
            .section.warnings h2 .count { background: #fff3e0; color: #e65100; }
            .section.success h2 .count { background: #e8f5e9; color: #2e7d32; }
            .entry { background: #fafafa; border-radius: 6px; padding: 12px 15px; margin-bottom: 10px; 
                    border-left: 4px solid #e0e0e0; }
            .section.failed .entry { border-left-color: #ef5350; background: #fff8f8; }
            .section.warnings .entry { border-left-color: #ff9800; background: #fffaf5; }
            .section.success .entry { border-left-color: #4caf50; background: #f8fff8; }
            .entry-header { font-weight: 600; margin-bottom: 6px; color: #333; }
            .entry-file { margin: 4px 0; }
            .entry-file a { color: #1976D2; text-decoration: none; }
            .entry-file a:hover { text-decoration: underline; }
            .entry-rows { color: #666; font-size: 13px; }
            .entry-error { color: #c62828; font-size: 13px; margin-top: 6px; }
            .error-detail { background: #f5f5f5; border: 1px solid #e0e0e0; border-radius: 4px; 
                           padding: 10px; margin-top: 8px; font-family: 'Monaco', 'Menlo', monospace; 
                           font-size: 11px; white-space: pre-wrap; overflow-x: auto; max-height: 300px; 
                           overflow-y: auto; }
            .action-items { background: #fff3e0; border-radius: 6px; padding: 15px; }
            .action-items ul { margin: 0; padding-left: 20px; }
            .action-items li { margin: 5px 0; color: #e65100; }
            .footer { text-align: center; padding: 15px 25px; background: #fafafa; 
                     font-size: 12px; color: #888; }
        """)
        html.append("</style>")
        html.append("</head>")
        html.append("<body>")
        html.append("<div class='container'>")
        
        # Header
        html.append("<div class='header'>")
        html.append("<h1>📊 Pricing Bot Summary</h1>")
        html.append(f"<div class='status'>{status} • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</div>")
        html.append("</div>")
        
        # Stats Grid
        html.append("<div class='stats-grid'>")
        html.append("<div class='stat-box'>")
        html.append("<h3>📧 Email Processing</h3>")
        html.append(f"<div class='value'>{email_uploads}</div>")
        html.append(f"<div class='detail'>{emails_processed} emails • {email_files_generated} files • {email_errors} errors</div>")
        html.append("</div>")
        html.append("<div class='stat-box'>")
        html.append("<h3>🌐 Web Scraping</h3>")
        html.append(f"<div class='value'>{scraper_uploads}</div>")
        html.append(f"<div class='detail'>{scrapers_processed} suppliers • {scraper_files_generated} files • {scraper_errors} errors</div>")
        html.append("</div>")
        html.append("<div class='stat-box'>")
        html.append("<h3>📊 Parsing Summary</h3>")
        html.append(f"<div class='value'>{total_valid_rows_all_files:,}</div>")
        html.append(f"<div class='detail'>{total_rows_all_files:,} total rows • {total_parsing_errors:,} parse errors</div>")
        html.append("</div>")
        
        # Reconciliation stats box (only if BigQuery reconciliation was used)
        if has_reconciliation:
            html.append("<div class='stat-box'>")
            html.append("<h3>🔄 Reconciliation</h3>")
            html.append(f"<div class='value'>{len(files_with_reconciliation)}</div>")
            html.append(f"<div class='detail'>{total_duplicates_found:,} duplicates • {total_synthetic_items:,} synthetic rows • {total_reconciliation_errors:,} errors</div>")
            html.append("</div>")
        
        html.append("</div>")
        
        # Section 1: FAILED
        if failed_emails:
            html.append("<div class='section failed'>")
            html.append(f"<h2>❌ Failed <span class='count'>{len(failed_emails)}</span></h2>")
            
            for result in failed_emails:
                email = result.email_result
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                html.append("<div class='entry'>")
                html.append(f"<div class='entry-header'>{email.supplier_name or 'UNKNOWN'} - {date_str} - {self._html_escape(email.subject[:60])}</div>")
                
                for error in result.errors:
                    html.append(f"<div class='entry-error'>• {self._html_escape(error[:200])}</div>")
                    # Check if this is a detailed error (header detection, etc.)
                    if '=' * 10 in error or 'HEADER DETECTION' in error.upper():
                        html.append(f"<div class='error-detail'>{self._html_escape(error)}</div>")
                
                html.append("</div>")
            
            html.append("</div>")
        
        # Section 2: WARNINGS
        if warning_files:
            html.append("<div class='section warnings'>")
            html.append(f"<h2>⚠️ Warnings <span class='count'>{len(warning_files)}</span></h2>")
            
            for entry in warning_files:
                date_str = entry.email_date.strftime('%d-%b-%y') if entry.email_date else 'Unknown'
                html.append("<div class='entry'>")
                html.append(f"<div class='entry-header'>{entry.supplier_name} - {date_str} - {self._html_escape(entry.subject[:60])}</div>")
                html.append(f"<div class='entry-file'>File: {self._html_escape(entry.filename)} ❌</div>")
                if entry.error:
                    # Check if this is a detailed error
                    if '=' * 10 in entry.error or 'HEADER DETECTION' in entry.error.upper():
                        html.append(f"<div class='entry-error'>Error: {self._html_escape(entry.error[:100])}...</div>")
                        html.append(f"<div class='error-detail'>{self._html_escape(entry.error)}</div>")
                    else:
                        html.append(f"<div class='entry-error'>Error: {self._html_escape(entry.error)}</div>")
                
                html.append("</div>")
            
            html.append("</div>")
        
        # Section 3: UNKNOWN SUPPLIERS
        if unknown_emails:
            html.append("<div class='section'>")
            html.append(f"<h2>❓ Unknown Suppliers <span class='count'>{len(unknown_emails)}</span></h2>")
            
            for result in unknown_emails:
                email = result.email_result
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                html.append("<div class='entry'>")
                html.append(f"<div class='entry-header'>{self._html_escape(email.from_address)} - {date_str}</div>")
                html.append(f"<div class='entry-file'>Subject: {self._html_escape(email.subject[:60])}</div>")
                html.append(f"<div class='entry-rows'>Domain: {email.from_domain}</div>")
                html.append("<div class='entry-error'>Unable to process - no supplier detected. Resend with SUPPLIER: GTAUTO or SUPPLIER: TECHNOPARTS in the email body (as appropriate).</div>")
                html.append(f"<div class='entry-rows'>Add to supplier_config.json if legitimate</div>")
                html.append("</div>")
            
            html.append("</div>")
        
        # Section 4: SKIPPED
        if skipped_emails:
            html.append("<div class='section'>")
            html.append(f"<h2>⏭️ Skipped <span class='count'>{len(skipped_emails)}</span></h2>")
            
            for result in skipped_emails:
                email = result.email_result
                date_str = email.date.strftime('%d-%b-%y') if email.date else 'Unknown'
                html.append("<div class='entry'>")
                html.append(f"<div class='entry-header'>{email.supplier_name or 'UNKNOWN'} - {date_str} - {self._html_escape(email.subject[:60])}</div>")
                
                is_scraper = email.from_address.startswith('scraper@')
                if is_scraper and hasattr(email, 'scraper_all_duplicates') and email.scraper_all_duplicates:
                    html.append("<div class='entry-rows'>Reason: No new files (all duplicates)</div>")
                elif not email.attachments:
                    html.append("<div class='entry-rows'>Reason: No attachments</div>")
                
                html.append("</div>")
            
            html.append("</div>")
        
        # Section 5: ACTION ITEMS
        action_items: List[str] = []
        
        if failed_emails:
            action_items.append(f"Review {len(failed_emails)} failed email(s) - check errors above")
        
        if warning_files:
            action_items.append(f"Review {len(warning_files)} file(s) with warnings")
        
        if unknown_emails:
            action_items.append(f"Review {len(unknown_emails)} unknown supplier(s) - add to config if needed")
        
        expired_count = sum(1 for r in results if r.expiry_is_past)
        if expired_count:
            action_items.append(f"{expired_count} file(s) with past expiry dates - verify with suppliers")
        
        fallback_count = sum(1 for r in results if r.brand_fallback_used)
        if fallback_count:
            action_items.append(f"{fallback_count} file(s) used fallback brand - consider adding keywords")
        
        if action_items:
            html.append("<div class='section'>")
            html.append("<h2>📋 Action Items</h2>")
            html.append("<div class='action-items'>")
            html.append("<ul>")
            for item in action_items:
                html.append(f"<li>{self._html_escape(item)}</li>")
            html.append("</ul>")
            html.append("</div>")
            html.append("</div>")
        
        # Section 6: SUCCESSFUL (at the end, compact format with clickable links)
        if success_files:
            html.append("<div class='section success'>")
            html.append(f"<h2>✅ Successful <span class='count'>{len(success_files)} files</span></h2>")
            
            for entry in success_files:
                date_str = entry.email_date.strftime('%d-%b-%y') if entry.email_date else 'Unknown'
                html.append("<div class='entry'>")
                html.append(f"<div class='entry-header'>{entry.supplier_name} - {date_str} - {self._html_escape(entry.subject[:60])}</div>")
                
                # File with clickable link
                if entry.drive_link:
                    html.append(f"<div class='entry-file'>File: <a href='{entry.drive_link}' target='_blank'>{self._html_escape(entry.filename)}</a></div>")
                else:
                    html.append(f"<div class='entry-file'>File: {self._html_escape(entry.filename)}</div>")
                
                if entry.total_rows > 0:
                    html.append(f"<div class='entry-rows'>Rows: {entry.valid_rows:,}/{entry.total_rows:,} valid</div>")
                
                html.append("</div>")
            
            html.append("</div>")
        
        # Footer
        html.append("<div class='footer'>")
        html.append("Email Pricing Bot v1.0.0")
        if results and results[-1].email_result.date:
            last_processed = results[-1].email_result.date.strftime('%Y-%m-%d %H:%M:%S UTC')
            html.append(f" • Last Processed: {last_processed}")
        html.append("</div>")
        
        html.append("</div>")  # container
        html.append("</body>")
        html.append("</html>")
        
        return "\n".join(html)
    
    @staticmethod
    def _html_escape(text: str) -> str:
        """Escape HTML special characters."""
        return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#x27;"))
    
    def send_summary_from_orchestrator_results(
        self,
        results: 'List[EmailProcessingResult]',
        recipients: List[str],
        dry_run: bool = False,
        from_email: Optional[str] = None
    ) -> bool:
        """
        Send summary email from orchestrator results.
        
        Args:
            results: List of EmailProcessingResult from orchestrator
            recipients: List of recipient email addresses
            dry_run: If True, log email but don't send
            from_email: Optional sender email address (e.g., pricing@ucalexports.com)
            
        Returns:
            True if sent successfully
        """
        try:
            # Build the summary text and HTML
            subject, text_body = self.build_summary_text_from_orchestrator_results(results)
            html_body = self.build_html_summary_from_orchestrator_results(results)
            
            # Calculate stats for logging using the split helper
            success_files, warning_files, failed_emails, skipped_emails, unknown_emails = \
                self._split_results_into_file_entries(results)
            
            emails_processed = len([r for r in results if not r.email_result.from_address.startswith('scraper@')])
            files_generated = sum(len(r.files_generated) for r in results)
            
            logger.info(
                "Preparing summary email (HTML + text)",
                emails_processed=emails_processed,
                successful_files=len(success_files),
                warning_files=len(warning_files),
                failed_emails=len(failed_emails),
                skipped_emails=len(skipped_emails)
            )
            
            if dry_run:
                logger.info(
                    "DRY RUN: Would send summary email",
                    recipients=recipients,
                    subject=subject,
                    text_body_length=len(text_body),
                    html_body_length=len(html_body)
                )
                logger.debug("Email body preview", body=text_body[:500])
                return True
            
            # Send email with both text and HTML body
            message_id = self.gmail_client.send_message(
                to=recipients,
                subject=subject,
                body=text_body,
                html_body=html_body,
                from_email=from_email
            )
            
            logger.info(
                "Summary email sent",
                message_id=message_id,
                recipients=recipients,
                emails_processed=emails_processed,
                files_generated=files_generated
            )
            
            return True
            
        except Exception as e:
            import traceback
            logger.error(
                "Failed to send summary email (send_enhanced_summary)",
                error=str(e),
                recipients=recipients
            )
            logger.error(f"Exception type: {type(e).__name__}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
