#!/usr/bin/env python3
"""
Historical Log Parser for load_historical_prices.py output.

Parses JSON log files from the historical price loader and generates a report containing:
1. Invalid filenames grouped by folder
2. Processing errors with file - detail format
3. Summary statistics

Usage:
    python scripts/parse_historical_log.py historical_file.log
    python scripts/parse_historical_log.py historical_file.log --output report.txt
"""

import json
import argparse
import re
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


def extract_invalid_filenames(entries: list[dict[str, Any]]) -> dict[str, list[str]]:
    """
    Extract invalid filename entries grouped by folder.
    
    Returns:
        Dict mapping folder name to list of invalid filenames
    """
    invalid_by_folder: dict[str, list[str]] = defaultdict(list)
    current_folder = "Unknown"
    
    for entry in entries:
        message = entry.get('message', '')
        
        # Track current folder
        folder_match = re.search(r'Processing folder: (\S+)', message)
        if folder_match:
            current_folder = folder_match.group(1)
            continue
        
        # Extract invalid filename
        if 'Invalid filename recorded:' in message:
            # Extract filename from message like "  Invalid filename recorded: FILENAME.csv"
            filename_match = re.search(r'Invalid filename recorded:\s*(.+)$', message)
            if filename_match:
                filename = filename_match.group(1).strip()
                invalid_by_folder[current_folder].append(filename)
    
    return dict(invalid_by_folder)


def extract_processing_errors(entries: list[dict[str, Any]]) -> list[tuple[str, str]]:
    """
    Extract processing errors (ERROR severity).
    
    Returns:
        List of tuples (filename, error_detail)
    """
    errors = []
    
    for entry in entries:
        if entry.get('severity') != 'ERROR':
            continue
        
        message = entry.get('message', '')
        
        # Match "Failed to process FILENAME.csv: ERROR_DETAILS"
        match = re.match(r'Failed to process (.+?):\s*(.+)$', message)
        if match:
            filename = match.group(1).strip()
            error_detail = match.group(2).strip()
            errors.append((filename, error_detail))
    
    return errors


def extract_summary(entries: list[dict[str, Any]]) -> dict[str, int]:
    """
    Extract summary statistics from the log.
    
    Returns:
        Dict with summary statistics
    """
    summary = {
        'folders_processed': 0,
        'files_found': 0,
        'files_processed': 0,
        'files_skipped': 0,
        'files_failed': 0,
        'invalid_filenames': 0
    }
    
    # Look for summary lines near the end
    for entry in entries:
        message = entry.get('message', '')
        
        if 'Folders processed:' in message:
            match = re.search(r'Folders processed:\s*(\d+)', message)
            if match:
                summary['folders_processed'] = int(match.group(1))
        elif 'Files found:' in message:
            match = re.search(r'Files found:\s*(\d+)', message)
            if match:
                summary['files_found'] = int(match.group(1))
        elif 'Files processed:' in message:
            match = re.search(r'Files processed:\s*(\d+)', message)
            if match:
                summary['files_processed'] = int(match.group(1))
        elif 'Files skipped:' in message:
            match = re.search(r'Files skipped:\s*(\d+)', message)
            if match:
                summary['files_skipped'] = int(match.group(1))
        elif 'Files failed:' in message:
            match = re.search(r'Files failed:\s*(\d+)', message)
            if match:
                summary['files_failed'] = int(match.group(1))
        elif 'Invalid filenames:' in message:
            match = re.search(r'Invalid filenames:\s*(\d+)', message)
            if match:
                summary['invalid_filenames'] = int(match.group(1))
    
    return summary


def generate_report(
    invalid_filenames: dict[str, list[str]],
    processing_errors: list[tuple[str, str]],
    summary: dict[str, int]
) -> str:
    """Generate the formatted report."""
    lines = []
    
    # Invalid filenames section
    lines.append("=== INVALID FILENAMES ===")
    lines.append("")
    
    if invalid_filenames:
        for folder, filenames in sorted(invalid_filenames.items()):
            lines.append(f"{folder} ({len(filenames)} files):")
            for filename in sorted(filenames):
                lines.append(f"  - {filename}")
            lines.append("")
    else:
        lines.append("No invalid filenames found.")
        lines.append("")
    
    # Processing errors section
    lines.append("=== PROCESSING ERRORS ===")
    lines.append("")
    
    if processing_errors:
        for filename, error_detail in processing_errors:
            lines.append(f"{filename} - {error_detail}")
        lines.append("")
    else:
        lines.append("No processing errors found.")
        lines.append("")
    
    # Summary section
    lines.append("=== SUMMARY ===")
    lines.append("")
    lines.append(f"Folders processed: {summary['folders_processed']}")
    lines.append(f"Files found: {summary['files_found']}")
    lines.append(f"Files processed: {summary['files_processed']}")
    lines.append(f"Files skipped: {summary['files_skipped']}")
    lines.append(f"Files failed: {summary['files_failed']}")
    lines.append(f"Invalid filenames: {summary['invalid_filenames']}")
    
    return '\n'.join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse historical price loader log files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/parse_historical_log.py historical_file.log
  python scripts/parse_historical_log.py historical_file.log --output report.txt
        """
    )
    
    parser.add_argument(
        'log_file',
        help='Path to the log file to parse'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Path to write the report (default: print to stdout)'
    )
    
    args = parser.parse_args()
    
    # Parse log file
    log_path = Path(args.log_file)
    if not log_path.exists():
        print(f"Error: Log file not found: {log_path}")
        return 1
    
    entries = parse_log_file(str(log_path))
    
    if not entries:
        print("Error: No valid log entries found in file")
        return 1
    
    # Extract data
    invalid_filenames = extract_invalid_filenames(entries)
    processing_errors = extract_processing_errors(entries)
    summary = extract_summary(entries)
    
    # Generate report
    report = generate_report(invalid_filenames, processing_errors, summary)
    
    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(report, encoding='utf-8')
        print(f"Report written to: {output_path}")
    else:
        print(report)
    
    return 0


if __name__ == "__main__":
    exit(main())
