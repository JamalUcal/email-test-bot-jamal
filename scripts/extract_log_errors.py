#!/usr/bin/env python3
"""
Extract ERROR and WARNING lines from JSON log files with context.

Usage:
    python scripts/extract_log_errors.py <log_file> [--output <output_file>]
    python scripts/extract_log_errors.py full run.log --output error_report.txt
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime


def parse_log_line(line: str) -> Dict[str, Any]:
    """Parse a JSON log line."""
    try:
        return json.loads(line.strip())
    except json.JSONDecodeError:
        return {"message": line.strip(), "severity": "UNKNOWN"}


def extract_errors_with_context(
    log_file: Path,
    context_lines: int = 10
) -> List[Dict[str, Any]]:
    """
    Extract ERROR and WARNING lines with surrounding context.
    
    Args:
        log_file: Path to log file
        context_lines: Number of lines before/after to include
        
    Returns:
        List of issue blocks with context
    """
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    parsed_lines = [parse_log_line(line) for line in lines]
    issues = []
    processed_indices = set()
    
    for idx, log_entry in enumerate(parsed_lines):
        severity = log_entry.get('severity', '').upper()
        
        if severity in ['ERROR', 'WARNING'] and idx not in processed_indices:
            # Calculate context range
            start = max(0, idx - context_lines)
            end = min(len(parsed_lines), idx + context_lines + 1)
            
            # Mark all lines in this block as processed (avoid duplicates)
            for i in range(start, end):
                processed_indices.add(i)
            
            # Build issue block
            issue_block = {
                'severity': severity,
                'line_number': idx + 1,
                'message': log_entry.get('message', ''),
                'timestamp': log_entry.get('timestamp', ''),
                'context_lines': parsed_lines[start:end],
                'context_start_line': start + 1,
                'context_end_line': end
            }
            issues.append(issue_block)
    
    return issues


def format_issue_block(issue: Dict[str, Any], block_num: int) -> str:
    """Format an issue block for output."""
    output = []
    output.append("=" * 80)
    output.append(f"ISSUE #{block_num}: {issue['severity']}")
    output.append(f"Line {issue['line_number']}: {issue['message']}")
    output.append(f"Timestamp: {issue['timestamp']}")
    output.append(f"Context: Lines {issue['context_start_line']}-{issue['context_end_line']}")
    output.append("=" * 80)
    output.append("")
    
    for idx, log_entry in enumerate(issue['context_lines'], start=issue['context_start_line']):
        severity = log_entry.get('severity', 'INFO')
        message = log_entry.get('message', str(log_entry))
        timestamp = log_entry.get('timestamp', '')
        
        # Highlight the actual error/warning line
        if idx == issue['line_number']:
            marker = ">>> "
        else:
            marker = "    "
        
        # Truncate long messages
        if len(message) > 200:
            message = message[:197] + "..."
        
        output.append(f"{marker}[{idx:4d}] [{severity:7s}] {message}")
    
    output.append("")
    return "\n".join(output)


def generate_report(log_file: Path, output_file: Path, context_lines: int = 10):
    """Generate error report from log file."""
    print(f"Analyzing log file: {log_file}")
    
    issues = extract_errors_with_context(log_file, context_lines)
    
    if not issues:
        print("No errors or warnings found!")
        return
    
    # Count by severity
    error_count = sum(1 for i in issues if i['severity'] == 'ERROR')
    warning_count = sum(1 for i in issues if i['severity'] == 'WARNING')
    
    print(f"Found {len(issues)} issues: {error_count} errors, {warning_count} warnings")
    print(f"Writing report to: {output_file}")
    
    with open(output_file, 'w') as f:
        # Write header
        f.write("=" * 80 + "\n")
        f.write("ERROR AND WARNING REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Log File: {log_file}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Total Issues: {len(issues)} ({error_count} errors, {warning_count} warnings)\n")
        f.write("=" * 80 + "\n\n")
        
        # Write issue blocks
        for idx, issue in enumerate(issues, 1):
            f.write(format_issue_block(issue, idx))
            f.write("\n\n")
    
    print(f"Report complete: {output_file}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract ERROR and WARNING lines from log files with context"
    )
    parser.add_argument('log_file', help='Path to log file')
    parser.add_argument(
        '--output', '-o',
        help='Output file path (default: <log_file>_errors.txt)',
        default=None
    )
    parser.add_argument(
        '--context', '-c',
        type=int,
        default=10,
        help='Number of context lines before/after each issue (default: 10)'
    )
    
    args = parser.parse_args()
    
    log_file = Path(args.log_file)
    if not log_file.exists():
        print(f"Error: Log file not found: {log_file}")
        sys.exit(1)
    
    # Determine output file
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = log_file.parent / f"{log_file.stem}_errors.txt"
    
    generate_report(log_file, output_file, args.context)


if __name__ == '__main__':
    main()

