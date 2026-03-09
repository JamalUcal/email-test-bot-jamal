"""
Report builder for summary emails.

Formats processing results into email reports.
"""

from typing import List
from datetime import datetime

from gmail.email_processor import ProcessingResults, EmailResult
from utils.logger import get_logger

logger = get_logger(__name__)


class ReportBuilder:
    """Builds summary email reports."""
    
    @staticmethod
    def build_text_report(results: ProcessingResults) -> str:
        """
        Build plain text summary report.
        
        Args:
            results: Processing results
            
        Returns:
            Plain text report
        """
        lines = []
        lines.append("=" * 70)
        lines.append("EMAIL PRICING BOT - PROCESSING SUMMARY")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append("")
        
        # Section 1: Statistics
        lines.append("STATISTICS")
        lines.append("-" * 70)
        lines.append(f"Emails Processed: {results.emails_processed}")
        lines.append(f"  - From Known Suppliers: {results.emails_from_suppliers}")
        lines.append(f"  - From Unknown Domains: {results.emails_unknown_domain}")
        lines.append(f"  - Ignored (Internal): {results.emails_ignored}")
        lines.append(f"Total Attachments: {results.total_attachments}")
        lines.append(f"  - Supported (CSV/XLSX): {results.supported_attachments}")
        lines.append(f"  - Warning (PDF/XLS): {results.warning_attachments}")
        lines.append("")
        
        # Section 2: Errors
        if results.errors:
            lines.append("ERRORS")
            lines.append("-" * 70)
            for error in results.errors:
                lines.append(f"  • {error}")
            lines.append("")
        
        # Section 3: Unknown Domains
        unknown_emails = [r for r in results.results if r.is_unknown_domain]
        if unknown_emails:
            lines.append("UNKNOWN DOMAINS (Not Configured)")
            lines.append("-" * 70)
            for email in unknown_emails:
                lines.append(f"  From: {email.from_address}")
                lines.append(f"  Domain: {email.from_domain}")
                lines.append(f"  Subject: {email.subject}")
                lines.append(f"  Attachments: {len(email.attachments)}")
                lines.append("")
        
        # Section 4: Supplier Emails Detected
        supplier_emails = [r for r in results.results if r.supplier_name]
        if supplier_emails:
            lines.append("SUPPLIER EMAILS DETECTED")
            lines.append("-" * 70)
            for email in supplier_emails:
                lines.append(f"  Supplier: {email.supplier_name}")
                lines.append(f"  From: {email.from_address}")
                lines.append(f"  Subject: {email.subject}")
                lines.append(f"  Date: {email.date.strftime('%Y-%m-%d %H:%M:%S')}")
                
                if email.supported_attachments:
                    lines.append(f"  Supported Attachments ({len(email.supported_attachments)}):")
                    for att in email.supported_attachments:
                        lines.append(f"    - {att.filename} ({att.size:,} bytes)")
                
                if email.warning_attachments:
                    lines.append(f"  Warning Attachments ({len(email.warning_attachments)}):")
                    for att in email.warning_attachments:
                        lines.append(f"    - {att.filename} (unsupported format)")
                
                lines.append("")
        
        # Section 5: Summary
        lines.append("=" * 70)
        lines.append("END OF REPORT")
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    @staticmethod
    def build_html_report(results: ProcessingResults) -> str:
        """
        Build HTML summary report.
        
        Args:
            results: Processing results
            
        Returns:
            HTML report
        """
        html = []
        html.append("<!DOCTYPE html>")
        html.append("<html>")
        html.append("<head>")
        html.append("<style>")
        html.append("body { font-family: Arial, sans-serif; margin: 20px; }")
        html.append("h1 { color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }")
        html.append("h2 { color: #555; margin-top: 30px; }")
        html.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }")
        html.append("th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }")
        html.append("th { background-color: #007bff; color: white; }")
        html.append("tr:nth-child(even) { background-color: #f2f2f2; }")
        html.append(".stats { background-color: #e7f3ff; padding: 15px; border-radius: 5px; }")
        html.append(".error { background-color: #ffe7e7; padding: 10px; border-left: 4px solid #dc3545; margin: 10px 0; }")
        html.append(".warning { background-color: #fff3cd; padding: 10px; border-left: 4px solid #ffc107; margin: 10px 0; }")
        html.append(".success { background-color: #d4edda; padding: 10px; border-left: 4px solid #28a745; margin: 10px 0; }")
        html.append("</style>")
        html.append("</head>")
        html.append("<body>")
        
        # Header
        html.append("<h1>📧 Email Pricing Bot - Processing Summary</h1>")
        html.append(f"<p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>")
        
        # Statistics
        html.append("<h2>📊 Statistics</h2>")
        html.append("<div class='stats'>")
        html.append(f"<p><strong>Emails Processed:</strong> {results.emails_processed}</p>")
        html.append(f"<p style='margin-left: 20px;'>• From Known Suppliers: {results.emails_from_suppliers}</p>")
        html.append(f"<p style='margin-left: 20px;'>• From Unknown Domains: {results.emails_unknown_domain}</p>")
        html.append(f"<p style='margin-left: 20px;'>• Ignored (Internal): {results.emails_ignored}</p>")
        html.append(f"<p><strong>Total Attachments:</strong> {results.total_attachments}</p>")
        html.append(f"<p style='margin-left: 20px;'>• Supported (CSV/XLSX): {results.supported_attachments}</p>")
        html.append(f"<p style='margin-left: 20px;'>• Warning (PDF/XLS): {results.warning_attachments}</p>")
        html.append("</div>")
        
        # Errors
        if results.errors:
            html.append("<h2>❌ Errors</h2>")
            for error in results.errors:
                html.append(f"<div class='error'>{error}</div>")
        
        # Unknown Domains
        unknown_emails = [r for r in results.results if r.is_unknown_domain]
        if unknown_emails:
            html.append("<h2>⚠️ Unknown Domains (Not Configured)</h2>")
            html.append("<table>")
            html.append("<tr><th>From</th><th>Domain</th><th>Subject</th><th>Attachments</th></tr>")
            for email in unknown_emails:
                html.append("<tr>")
                html.append(f"<td>{email.from_address}</td>")
                html.append(f"<td>{email.from_domain}</td>")
                html.append(f"<td>{email.subject}</td>")
                html.append(f"<td>{len(email.attachments)}</td>")
                html.append("</tr>")
            html.append("</table>")
        
        # Supplier Emails
        supplier_emails = [r for r in results.results if r.supplier_name]
        if supplier_emails:
            html.append("<h2>✅ Supplier Emails Detected</h2>")
            for email in supplier_emails:
                html.append("<div class='success'>")
                html.append(f"<p><strong>Supplier:</strong> {email.supplier_name}</p>")
                html.append(f"<p><strong>From:</strong> {email.from_address}</p>")
                html.append(f"<p><strong>Subject:</strong> {email.subject}</p>")
                html.append(f"<p><strong>Date:</strong> {email.date.strftime('%Y-%m-%d %H:%M:%S')}</p>")
                
                if email.supported_attachments:
                    html.append(f"<p><strong>Supported Attachments ({len(email.supported_attachments)}):</strong></p>")
                    html.append("<ul>")
                    for att in email.supported_attachments:
                        html.append(f"<li>{att.filename} ({att.size:,} bytes)</li>")
                    html.append("</ul>")
                
                if email.warning_attachments:
                    html.append(f"<p><strong>Warning Attachments ({len(email.warning_attachments)}):</strong></p>")
                    html.append("<ul>")
                    for att in email.warning_attachments:
                        html.append(f"<li>{att.filename} (unsupported format)</li>")
                    html.append("</ul>")
                
                html.append("</div>")
        
        html.append("</body>")
        html.append("</html>")
        
        return "\n".join(html)
