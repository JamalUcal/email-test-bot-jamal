#!/usr/bin/env python3
"""
Log Parser for Email Pricing Bot

Parses JSON log files and generates a human-readable report containing:
1. List of all Google Drive URLs from successful uploads
2. Errors and warnings grouped by type

Usage:
    python scripts/parse_log.py output.log
    python scripts/parse_log.py output.log --output report.md
"""

import json
import argparse
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Any


def parse_log_file(log_path: str) -> list[dict[str, Any]]:
    """Parse a log file containing JSON lines."""
    entries = []
    with open(log_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            # Skip non-JSON lines (e.g., shell prompts)
            if not line.startswith('{'):
                continue
            try:
                entry = json.loads(line)
                entry['_line_num'] = line_num
                entries.append(entry)
            except json.JSONDecodeError:
                # Skip lines that aren't valid JSON
                continue
    return entries


def extract_drive_urls(entries: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Extract all Google Drive URLs from successful upload entries."""
    urls = []
    for entry in entries:
        if entry.get('message') == 'File uploaded successfully' and 'web_link' in entry:
            urls.append({
                'url': entry['web_link'],
                'filename': entry.get('filename', 'unknown'),
                'file_id': entry.get('file_id', ''),
                'timestamp': entry.get('timestamp', '')
            })
    return urls


def categorize_issue(entry: dict[str, Any], context: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    """Categorize a warning/error entry and extract relevant details.
    
    Args:
        entry: The log entry to categorize
        context: Optional context dict with 'current_file' tracking
    """
    message = entry.get('message', '')
    if context is None:
        context = {}
    
    # Drive folder not found - only if we have structured fields (brand AND folder_id)
    # This avoids matching summary messages like "Drive upload failed: Google Drive folder not found..."
    if ('Google Drive folder not found' in message and 
        entry.get('brand') and entry.get('folder_id')):
        return 'Drive Folder Not Found', {
            'brand': entry.get('brand'),
            'folder_id': entry.get('folder_id'),
            'filename': entry.get('file_path', '').split('/')[-1] if entry.get('file_path') else entry.get('filename', 'Unknown'),
            'error': entry.get('error', message)
        }
    
    # High parsing error rate - include filename and subject from context
    if 'HIGH PARSING ERROR RATE' in message:
        return 'High Parsing Error Rate', {
            'error_rate': entry.get('error_rate', 0),
            'error_count': entry.get('error_count', 0),
            'total_rows': entry.get('total_rows', 0),
            'threshold': entry.get('threshold', 0.02),
            'message': message,
            'filename': context.get('current_file', 'Unknown file'),
            'subject': context.get('current_subject', ''),
            'supplier': context.get('current_supplier', '')
        }
    
    # File already exists
    if 'already exists in Drive folder' in message:
        return 'File Already Exists in Drive', {
            'filename': message.split("'")[1] if "'" in message else 'Unknown',
            'existing_file_id': entry.get('existing_file_id', 'Unknown')
        }
    
    # Duplicate header - include filename from context for filtering later
    if 'Duplicate header' in message:
        return 'Duplicate Header Detected', {
            'message': message,
            'filename': context.get('current_file', 'Unknown file')
        }
    
    # Header detection failed
    if 'HEADER DETECTION FAILED' in message or 'Header detection failed' in message.lower():
        return 'Header Detection Failed', {
            'message': message,
            'details': extract_header_failure_details(message),
            'subject': context.get('current_subject', ''),
            'supplier': context.get('current_supplier', '')
        }
    
    # Currency ambiguous
    if 'Currency ambiguous' in message:
        return 'Currency Ambiguous', {
            'message': message
        }
    
    # Brand detection failed
    if 'Brand detection failed' in message:
        return 'Brand Detection Failed', {
            'message': message
        }
    
    # Configuration error
    if 'Configuration error' in message or 'No configuration found' in message:
        return 'Configuration Error', {
            'message': message
        }
    
    # CSV generation failed
    if 'CSV generation failed' in message:
        details = extract_header_failure_details(message)
        return 'CSV Generation Failed', {
            'message': message[:200] + '...' if len(message) > 200 else message,
            'details': details,
            'subject': context.get('current_subject', ''),
            'supplier': context.get('current_supplier', '')
        }
    
    # Drive upload failed
    if 'Drive upload failed' in message:
        return 'Drive Upload Failed', {
            'message': message
        }
    
    # Email processed with warnings (unknown supplier)
    if 'Email processed with warnings' in message and entry.get('is_unknown_domain'):
        return 'Unknown Supplier/Domain', {
            'supplier': entry.get('supplier', 'UNKNOWN'),
            'from_address': entry.get('from_address', ''),
            'subject': entry.get('subject', ''),
            'original_sender': entry.get('original_sender', '')
        }
    
    # Email file parsing failed
    if 'Email file parsing failed' in message or 'Email processed with some files failed' in message:
        return 'File Processing Failed', {
            'supplier': entry.get('supplier', 'Unknown'),
            'subject': entry.get('subject', ''),
            'files_failed': entry.get('files_failed', []),
            'files_generated': entry.get('files_generated', [])
        }
    
    # Failed to check for existing file
    if 'Failed to check for existing file' in message:
        return 'Drive API Error', {
            'filename': entry.get('filename', 'Unknown'),
            'folder_id': entry.get('folder_id', 'Unknown'),
            'message': message[:200] + '...' if len(message) > 200 else message
        }
    
    # BigQuery processing failed (direct BigQuery errors)
    if 'BigQuery price list processing failed' in message or 'BigQueryProcessingError' in message:
        return 'BigQuery Processing Failed', {
            'price_list_id': entry.get('price_list_id'),
            'error': entry.get('error', message),
            'supplier': context.get('current_supplier', ''),
            'subject': context.get('current_subject', '')
        }
    
    # GCS/Storage errors
    if ('Failed to delete GCS' in message or 
        ('Failed to upload' in message and 'GCS' in message) or
        'Failed to download' in message):
        return 'GCS Storage Error', {
            'message': message,
            'price_list_id': entry.get('price_list_id', '')
        }
    
    # BigQuery record update failures
    if 'Failed to update BigQuery' in message or 'Failed to update price_list' in message:
        return 'BigQuery Update Error', {
            'message': message,
            'price_list_id': entry.get('price_list_id', '')
        }
    
    # Generic fallback
    return 'Other Issues', {
        'message': message[:300] + '...' if len(message) > 300 else message,
        'severity': entry.get('severity', 'UNKNOWN')
    }


def extract_header_failure_details(message: str) -> dict[str, Any]:
    """Extract structured details from header detection failure messages."""
    details = {}
    
    # Extract filename
    filename_match = re.search(r'HEADER DETECTION FAILED: (.+?)\n', message)
    if filename_match:
        details['filename'] = filename_match.group(1)
    
    # Extract missing fields
    missing_match = re.search(r'Missing required: (.+?)(?:\n|$)', message)
    if missing_match:
        details['missing_fields'] = missing_match.group(1)
    
    # Extract matched fields
    matched_match = re.search(r'Successfully matched: (.+?)(?:\n|$)', message)
    if matched_match:
        details['matched_fields'] = matched_match.group(1)
    
    return details


def extract_failure_reason(failure_msg: str) -> tuple[str, str]:
    """Extract a clean failure reason from a failure message.
    
    Returns (filename, reason) tuple.
    """
    # Split on first colon to get filename and reason
    if ': ' in failure_msg:
        parts = failure_msg.split(': ', 1)
        filename = parts[0]
        reason = parts[1] if len(parts) > 1 else 'Unknown reason'
    else:
        filename = failure_msg
        reason = 'Unknown reason'
    
    # Categorize the reason
    if 'Drive upload failed' in reason or 'folder not found' in reason.lower():
        return filename, 'DRIVE_FOLDER_ERROR'
    elif 'Currency ambiguous' in reason:
        return filename, 'CURRENCY_AMBIGUOUS'
    elif 'HEADER DETECTION FAILED' in reason or 'CSV generation failed' in reason:
        # Extract the actual missing fields
        details = extract_header_failure_details(reason)
        if details.get('missing_fields'):
            return filename, f"Header detection failed - Missing: {details['missing_fields']}"
        return filename, 'Header detection failed'
    elif 'Configuration error' in reason or 'No configuration found' in reason:
        # Extract brand/supplier info
        match = re.search(r"brand '(\w+)'.*supplier '(\w+)'", reason)
        if match:
            return filename, f"No config for brand '{match.group(1)}' in supplier '{match.group(2)}'"
        return filename, reason[:100]
    elif 'Brand detection failed' in reason:
        return filename, 'Brand detection failed'
    else:
        # Truncate other reasons sensibly
        return filename, reason[:100] + ('...' if len(reason) > 100 else '')


def collect_issues(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Collect and group all warnings and errors by type."""
    issues = defaultdict(list)
    
    # Track context as we process entries (for filename and subject association)
    context: dict[str, str | None] = {'current_file': None, 'current_subject': None, 'current_supplier': None}
    
    # First pass: collect all issues with context tracking
    for entry in entries:
        message = entry.get('message', '')
        
        # Update context: track current email subject and supplier
        # Look for "SUPPLIER |" entries which have subject field
        if 'SUPPLIER |' in message and entry.get('subject'):
            context['current_subject'] = entry.get('subject')
            context['current_supplier'] = entry.get('supplier')
        
        # Update context: track current file being processed
        # Look for "Detecting headers in:" or "Starting streaming CSV generation"
        if 'Detecting headers in:' in message:
            match = re.search(r'Detecting headers in: (.+?) \(', message)
            if match:
                context['current_file'] = match.group(1)
        elif 'Starting streaming CSV generation' in message:
            output_file = entry.get('output_file', '')
            if output_file:
                context['current_file'] = output_file.split('/')[-1]
        elif 'Streaming CSV generation complete' in message:
            output_file = entry.get('output_file', '')
            if output_file:
                context['current_file'] = output_file.split('/')[-1]
        
        severity = entry.get('severity', '')
        if severity not in ('WARNING', 'ERROR'):
            continue
        
        category, details = categorize_issue(entry, context)
        details['timestamp'] = entry.get('timestamp', '')
        details['severity'] = severity
        details['_line_num'] = entry.get('_line_num', 0)
        issues[category].append(details)
    
    # Second pass: identify files with folder errors to avoid duplication
    folder_error_files = set()
    for issue in issues.get('Drive Folder Not Found', []):
        filename = issue.get('filename', '')
        if filename:
            folder_error_files.add(filename)
    
    # Also track folder IDs that are known to be missing
    missing_folder_ids = set()
    for issue in issues.get('Drive Folder Not Found', []):
        folder_id = issue.get('folder_id', '')
        if folder_id and folder_id != 'Unknown':
            missing_folder_ids.add(folder_id)
    
    # Filter out "Drive API Error" entries that are just precursors to folder errors
    if 'Drive API Error' in issues:
        filtered_api_errors = []
        for issue in issues['Drive API Error']:
            folder_id = issue.get('folder_id', '')
            # Only keep if the folder ID isn't in our known missing list
            if folder_id not in missing_folder_ids:
                filtered_api_errors.append(issue)
        if filtered_api_errors:
            issues['Drive API Error'] = filtered_api_errors
        else:
            del issues['Drive API Error']
    
    # Filter out "Drive Upload Failed" as it duplicates "Drive Folder Not Found"
    if 'Drive Upload Failed' in issues:
        del issues['Drive Upload Failed']
    
    # Build set of files that actually failed (for filtering duplicate header warnings)
    failed_files = set()
    for issue in issues.get('Header Detection Failed', []):
        details = issue.get('details', {})
        if details.get('filename'):
            failed_files.add(details['filename'])
    for issue in issues.get('CSV Generation Failed', []):
        details = issue.get('details', {})
        if details.get('filename'):
            failed_files.add(details['filename'])
    
    # Filter "Duplicate Header Detected" - only keep if file actually failed
    # (duplicate headers are expected and handled gracefully for successful files)
    if 'Duplicate Header Detected' in issues:
        filtered_duplicates = []
        for issue in issues['Duplicate Header Detected']:
            filename = issue.get('filename', '')
            # Only keep if this file is in the failed files list
            if filename in failed_files:
                filtered_duplicates.append(issue)
        if filtered_duplicates:
            issues['Duplicate Header Detected'] = filtered_duplicates
        else:
            del issues['Duplicate Header Detected']
    
    return dict(issues)


def format_issue_section(category: str, issues: list[dict[str, Any]], 
                         known_folder_error_files: set[str] | None = None) -> str:
    """Format a section for a specific issue category.
    
    Args:
        category: The issue category name
        issues: List of issues in this category
        known_folder_error_files: Set of filenames already reported as folder errors
    """
    if known_folder_error_files is None:
        known_folder_error_files = set()
    
    lines = []
    count = len(issues)
    severity_counts = defaultdict(int)
    for issue in issues:
        severity_counts[issue.get('severity', 'UNKNOWN')] += 1
    
    severity_str = ', '.join(f"{count} {sev}" for sev, count in severity_counts.items())
    lines.append(f"### {category} ({count} occurrences - {severity_str})")
    lines.append("")
    
    if category == 'Drive Folder Not Found':
        # Group by brand/folder
        by_brand = defaultdict(list)
        for issue in issues:
            key = (issue.get('brand', 'Unknown'), issue.get('folder_id', 'Unknown'))
            by_brand[key].append(issue.get('filename', 'Unknown'))
        
        for (brand, folder_id), filenames in by_brand.items():
            lines.append(f"**Brand:** {brand} | **Folder ID:** `{folder_id}`")
            for fn in sorted(set(filenames)):
                lines.append(f"- {fn}")
            lines.append("")
    
    elif category == 'High Parsing Error Rate':
        for issue in issues:
            filename = issue.get('filename', 'Unknown file')
            subject = issue.get('subject', '')
            supplier = issue.get('supplier', '')
            rate = issue.get('error_rate', 0) * 100
            error_count = issue.get('error_count', 0)
            total = issue.get('total_rows', 0)
            threshold = issue.get('threshold', 0.02) * 100
            lines.append(f"- **File:** `{filename}`")
            if subject:
                lines.append(f"  - Subject: {subject}")
            if supplier:
                lines.append(f"  - Supplier: {supplier}")
            lines.append(f"  - Error rate: {rate:.1f}% ({error_count:,}/{total:,} rows failed)")
            lines.append(f"  - Threshold: {threshold}%")
            lines.append("")
    
    elif category == 'File Already Exists in Drive':
        # Deduplicate and list
        seen = set()
        for issue in issues:
            fn = issue.get('filename', 'Unknown')
            if fn not in seen:
                seen.add(fn)
                lines.append(f"- {fn}")
        lines.append("")
    
    elif category == 'Duplicate Header Detected':
        for issue in issues:
            lines.append(f"- {issue.get('message', 'Unknown')}")
        lines.append("")
    
    elif category == 'Header Detection Failed' or category == 'CSV Generation Failed':
        seen_files = set()
        for issue in issues:
            details = issue.get('details', {})
            filename = details.get('filename', 'Unknown file')
            if filename in seen_files:
                continue
            seen_files.add(filename)
            
            lines.append(f"**File:** {filename}")
            subject = issue.get('subject', '')
            supplier = issue.get('supplier', '')
            if subject:
                lines.append(f"- Subject: {subject}")
            if supplier:
                lines.append(f"- Supplier: {supplier}")
            if details.get('missing_fields'):
                lines.append(f"- Missing: {details['missing_fields']}")
            if details.get('matched_fields'):
                lines.append(f"- Found: {details['matched_fields']}")
            lines.append("")
    
    elif category == 'Currency Ambiguous':
        seen = set()
        for issue in issues:
            msg = issue.get('message', '')
            if msg not in seen:
                seen.add(msg)
                lines.append(f"- {msg}")
        lines.append("")
    
    elif category == 'Unknown Supplier/Domain':
        for issue in issues:
            subject = issue.get('subject', 'No subject')
            from_addr = issue.get('from_address', 'Unknown')
            original = issue.get('original_sender', '')
            lines.append(f"- **Subject:** {subject}")
            lines.append(f"  - From: {from_addr}" + (f" (original: {original})" if original else ""))
        lines.append("")
    
    elif category == 'File Processing Failed':
        for issue in issues:
            supplier = issue.get('supplier', 'Unknown')
            subject = issue.get('subject', 'No subject')
            files_failed = issue.get('files_failed', [])
            files_generated = issue.get('files_generated', [])
            
            # Filter out failures that are already reported elsewhere (folder errors)
            unique_failures = []
            for fail in files_failed:
                filename, reason = extract_failure_reason(fail)
                # Skip if this is a folder error (already in "Drive Folder Not Found")
                if reason == 'DRIVE_FOLDER_ERROR':
                    continue
                # Skip if filename is in known folder error files
                if filename in known_folder_error_files:
                    continue
                unique_failures.append((filename, reason))
            
            # Skip this entry entirely if all failures were folder errors
            if not unique_failures and not files_generated:
                continue
            if not unique_failures:
                # All failures were folder errors, nothing unique to report
                continue
            
            lines.append(f"**Supplier:** {supplier}")
            lines.append(f"**Subject:** {subject}")
            if files_generated:
                lines.append(f"- Generated: {len(files_generated)} files")
            if unique_failures:
                lines.append(f"- Failed ({len(unique_failures)}):")
                seen_reasons = {}
                for filename, reason in unique_failures[:5]:
                    # Group by reason to avoid repetition
                    if reason not in seen_reasons:
                        seen_reasons[reason] = []
                    seen_reasons[reason].append(filename)
                
                for reason, filenames in seen_reasons.items():
                    if len(filenames) == 1:
                        lines.append(f"  - `{filenames[0]}`: {reason}")
                    else:
                        lines.append(f"  - {reason}:")
                        for fn in filenames[:3]:
                            lines.append(f"    - `{fn}`")
                        if len(filenames) > 3:
                            lines.append(f"    - ... and {len(filenames) - 3} more")
                            
                if len(unique_failures) > 5:
                    lines.append(f"  - ... and {len(unique_failures) - 5} more")
            lines.append("")
    
    elif category == 'Configuration Error' or category == 'Brand Detection Failed':
        seen = set()
        for issue in issues:
            msg = issue.get('message', '')
            if msg not in seen:
                seen.add(msg)
                lines.append(f"- {msg}")
        lines.append("")
    
    elif category == 'BigQuery Processing Failed':
        for issue in issues:
            price_list_id = issue.get('price_list_id', 'Unknown')
            error = issue.get('error', 'Unknown error')
            # Truncate UUID for readability
            display_id = f"{price_list_id[:8]}..." if price_list_id and len(price_list_id) > 8 else price_list_id
            lines.append(f"**Price List ID:** `{display_id}`")
            if issue.get('subject'):
                lines.append(f"- Subject: {issue['subject']}")
            if issue.get('supplier'):
                lines.append(f"- Supplier: {issue['supplier']}")
            lines.append(f"- Error: {error[:200]}{'...' if len(str(error)) > 200 else ''}")
            lines.append("")
    
    elif category in ('GCS Storage Error', 'BigQuery Update Error'):
        seen = set()
        for issue in issues:
            msg = issue.get('message', '')
            if msg not in seen:
                seen.add(msg)
                lines.append(f"- {msg[:150]}{'...' if len(msg) > 150 else ''}")
        lines.append("")
    
    else:
        # Generic formatting
        for issue in issues[:10]:  # Limit to first 10
            msg = issue.get('message', 'No message')
            lines.append(f"- {msg}")
        if len(issues) > 10:
            lines.append(f"- ... and {len(issues) - 10} more")
        lines.append("")
    
    return '\n'.join(lines)


def generate_report(log_path: str, entries: list[dict[str, Any]], 
                    urls: list[dict[str, str]], issues: dict[str, list[dict[str, Any]]]) -> str:
    """Generate the full markdown report."""
    lines = []
    
    # Header
    lines.append("# Email Processing Log Report")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Log file:** `{log_path}`")
    lines.append("")
    
    # Summary
    total_warnings = sum(len(v) for k, v in issues.items() 
                        if any(i.get('severity') == 'WARNING' for i in v))
    total_errors = sum(len(v) for k, v in issues.items() 
                      if any(i.get('severity') == 'ERROR' for i in v))
    
    lines.append("## Summary")
    lines.append(f"- **Total log entries:** {len(entries):,}")
    lines.append(f"- **Files uploaded:** {len(urls)}")
    lines.append(f"- **Warning entries:** {sum(1 for e in entries if e.get('severity') == 'WARNING')}")
    lines.append(f"- **Error entries:** {sum(1 for e in entries if e.get('severity') == 'ERROR')}")
    lines.append(f"- **Issue categories:** {len(issues)}")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Google Drive URLs
    lines.append(f"## Google Drive URLs ({len(urls)} files)")
    lines.append("")
    if urls:
        for url_info in urls:
            lines.append(url_info['url'])
    else:
        lines.append("*No files were uploaded to Google Drive.*")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # Errors & Warnings by Type
    lines.append("## Errors & Warnings by Type")
    lines.append("")
    
    if not issues:
        lines.append("*No errors or warnings found.*")
    else:
        # Build set of files with folder errors to avoid duplication
        folder_error_files = set()
        for issue in issues.get('Drive Folder Not Found', []):
            filename = issue.get('filename', '')
            if filename:
                folder_error_files.add(filename)
        
        # Sort categories: errors first, then by count
        def sort_key(item):
            category, issue_list = item
            has_errors = any(i.get('severity') == 'ERROR' for i in issue_list)
            return (0 if has_errors else 1, -len(issue_list), category)
        
        sorted_issues = sorted(issues.items(), key=sort_key)
        
        for category, issue_list in sorted_issues:
            section = format_issue_section(category, issue_list, folder_error_files)
            # Only add section if it has content beyond the header
            section_lines = section.strip().split('\n')
            if len(section_lines) > 2:  # Header + blank line + content
                lines.append(section)
    
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Parse email pricing bot log files and generate a report'
    )
    parser.add_argument('log_file', help='Path to the log file to parse')
    parser.add_argument(
        '--output', '-o',
        default='log_report.md',
        help='Output file path (default: log_report.md)'
    )
    parser.add_argument(
        '--urls-only',
        action='store_true',
        help='Only output URLs (one per line, no markdown)'
    )
    
    args = parser.parse_args()
    
    log_path = Path(args.log_file)
    if not log_path.exists():
        print(f"Error: Log file not found: {log_path}")
        return 1
    
    print(f"Parsing log file: {log_path}")
    entries = parse_log_file(str(log_path))
    print(f"  Found {len(entries):,} log entries")
    
    urls = extract_drive_urls(entries)
    print(f"  Found {len(urls)} Google Drive URLs")
    
    if args.urls_only:
        # Simple URL output mode
        output_path = Path(args.output)
        with open(output_path, 'w') as f:
            for url_info in urls:
                f.write(url_info['url'] + '\n')
        print(f"URLs written to: {output_path}")
        return 0
    
    issues = collect_issues(entries)
    print(f"  Found {len(issues)} issue categories")
    for cat, items in sorted(issues.items(), key=lambda x: -len(x[1])):
        print(f"    - {cat}: {len(items)}")
    
    report = generate_report(str(log_path), entries, urls, issues)
    
    output_path = Path(args.output)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nReport generated: {output_path}")
    return 0


if __name__ == '__main__':
    exit(main())
