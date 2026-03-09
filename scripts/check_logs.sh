#!/bin/bash
# Convenience script for checking scraper logs

LOG_FILE="${1:-full run.log}"
OUTPUT_FILE="${2:-error_report.txt}"

echo "Extracting errors from: $LOG_FILE"
python scripts/extract_log_errors.py "$LOG_FILE" --output "$OUTPUT_FILE"

if [ -f "$OUTPUT_FILE" ]; then
    echo ""
    echo "Report generated: $OUTPUT_FILE"
    echo ""
    echo "Summary (first 50 lines):"
    head -50 "$OUTPUT_FILE"
fi

