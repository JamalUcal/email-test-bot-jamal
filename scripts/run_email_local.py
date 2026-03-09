#!/usr/bin/env python3
"""
Local email processor runner for development and testing.

Allows running email processing locally with various options including:
- Dry run mode (no state updates, no actual emails sent)
- Force execution (ignore schedule)
- Test config (use test configurations)
"""

import sys
import os
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gmail.gmail_client import GmailClient
from orchestrator import ProcessingOrchestrator
from notification.email_sender import EmailSender
from utils.state_manager import StateManager
from utils.logger import setup_logger
from datetime import datetime, timezone
import json

logger = setup_logger(__name__)


def load_local_configs(use_test_config: bool = False):
    """
    Load configuration files from local filesystem.
    
    Args:
        use_test_config: If True, load test configurations
        
    Returns:
        Dictionary with all configs
    """
    config_dir = Path(__file__).parent.parent / "config"
    
    # Load core config (test or production)
    core_filename = "core_config_test.json" if use_test_config else "core_config.json"
    core_file = config_dir / "core" / core_filename
    logger.info(f"Loading core config from: {core_filename}")
    with open(core_file) as f:
        core_config = json.load(f)
    
    # Load supplier config
    with open(config_dir / "supplier" / "supplier_config.json") as f:
        supplier_config = json.load(f)
    
    # Load brand config (test or production)
    brand_filename = "brand_config_test.json" if use_test_config else "brand_config.json"
    brand_file = config_dir / "brand" / brand_filename
    logger.info(f"Loading brand config from: {brand_filename}")
    with open(brand_file) as f:
        brand_config = json.load(f)
    
    # Load column mapping config
    column_mapping_file = config_dir / "core" / "column_mapping_config.json"
    logger.info(f"Loading column mapping config from: column_mapping_config.json")
    with open(column_mapping_file) as f:
        column_mapping_config = json.load(f)
    
    # Load currency config
    currency_config_file = config_dir / "core" / "currency_config.json"
    logger.info(f"Loading currency config from: currency_config.json")
    with open(currency_config_file) as f:
        currency_config = json.load(f)
    
    config_mode = "TEST" if use_test_config else "PRODUCTION"
    logger.info(f"Loaded configs in {config_mode} mode")
    
    return {
        'core': core_config,
        'suppliers': supplier_config,
        'brands': brand_config,
        'column_mapping': column_mapping_config,
        'currency': currency_config
    }


def run_email_processor(args):
    """
    Run email processor with specified options.
    
    Args:
        args: Parsed command line arguments
    """
    logger.info("Local email processor starting...")
    logger.info(f"  Dry run: {args.dry_run}")
    logger.info(f"  Skip BigQuery: {args.skip_bigquery}")
    logger.info(f"  Force execution: {args.force}")
    logger.info(f"  Test config: {args.use_test_config}")
    logger.info(f"  Max emails: {args.max_emails}")
    logger.info(f"  End date: {args.end_date}")
    
    # Validate that --force requires --start-date
    if args.force and not args.start_date:
        logger.error("Error: --force requires --start-date parameter")
        logger.error("Usage: python scripts/run_email_local.py --force --start-date 2026-01-01 --dry-run")
        sys.exit(1)
    
    # Load configurations
    config = load_local_configs(use_test_config=args.use_test_config)
    
    # Initialize state manager (local mode)
    state_manager = StateManager(
        bucket_name="dummy",
        state_file_path="email_state.json",
        use_local=True,
        local_path="./state"
    )
    
    # Load service account credentials from GOOGLE_APPLICATION_CREDENTIALS env var
    service_account_info = {}
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if credentials_path and os.path.exists(credentials_path):
        logger.info(f"Loading service account from: {credentials_path}")
        with open(credentials_path) as f:
            service_account_info = json.load(f)
    else:
        logger.error(
            "No service account credentials found. "
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
        sys.exit(1)
    
    # Initialize Gmail client
    delegated_user = config['core']['gmail']['delegated_user_email']
    logger.info(f"Initializing Gmail client (delegated user: {delegated_user})")
    
    gmail_client = GmailClient(
        service_account_info=service_account_info,
        delegated_user=delegated_user
    )
    
    # Initialize processing orchestrator
    logger.info("Initializing processing orchestrator")
    orchestrator = ProcessingOrchestrator(
        gmail_client=gmail_client,
        supplier_configs=config['suppliers'],
        brand_configs=config['brands'],
        core_config=config['core'],
        service_account_info=service_account_info,
        column_mapping_config=config['column_mapping'],
        currency_config=config['currency']
    )
    
    # Determine after_date based on --start-date or state file
    state = state_manager.get_state()
    after_date = None
    
    if args.start_date:
        # Use provided start date (with --force)
        try:
            # Parse date and make it timezone-aware (UTC)
            parsed_date = datetime.strptime(args.start_date, '%Y-%m-%d')
            after_date = parsed_date.replace(tzinfo=timezone.utc)
            logger.info(f"Using provided start date: {after_date.isoformat()}")
        except ValueError as e:
            logger.error(f"Invalid date format: {args.start_date}. Use YYYY-MM-DD format.")
            sys.exit(1)
    elif state and 'last_processed_timestamp' in state:
        # Use state file timestamp (normal operation)
        after_date = datetime.fromisoformat(state['last_processed_timestamp'].replace('Z', '+00:00'))
        logger.info(f"Processing emails after: {after_date.isoformat()}")
    else:
        # No state and no start date - process all
        after_date = None
        logger.info("Processing all available emails (no date filter)")
    
    # Parse end date if provided (overrides default 7-day window)
    before_date = None
    if args.end_date:
        try:
            # Parse date and make it timezone-aware (UTC)
            parsed_end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
            before_date = parsed_end_date.replace(tzinfo=timezone.utc)
            logger.info(f"Using provided end date: {before_date.isoformat()}")
        except ValueError as e:
            logger.error(f"Invalid date format: {args.end_date}. Use YYYY-MM-DD format.")
            sys.exit(1)
    
    # Process emails
    logger.info("Starting email processing...")
    
    try:
        results, final_search_date = orchestrator.process_emails(
            after_date=after_date,
            before_date=before_date,
            max_emails=args.max_emails,
            dry_run=args.dry_run,
            skip_bigquery=args.skip_bigquery
        )
        
        # Log results - classify based on actual file parsing success
        # A result is successful only if it has at least one file WITHOUT an error
        successful = [
            r for r in results 
            if r.files_generated and any(f.error is None for f in r.files_generated)
        ]
        
        # Files that failed within an email (all files had errors)
        file_failures = [
            r for r in results
            if r.files_generated and all(f.error is not None for f in r.files_generated)
        ]
        
        # Emails with warnings or unknown domains
        warnings = [r for r in results if r.warnings or r.email_result.is_unknown_domain]
        
        # Emails with email-level errors (no files attempted)
        errors = [r for r in results if r.errors]
        
        # Count actual generated files vs failed files
        total_generated = sum(
            sum(1 for f in r.files_generated if f.error is None) 
            for r in results
        )
        total_file_errors = sum(
            sum(1 for f in r.files_generated if f.error is not None) 
            for r in results
        )
        
        logger.info(
            "Processing complete",
            total_emails=len(results),
            successful_emails=len(successful),
            file_failure_emails=len(file_failures),
            warning_emails=len(warnings),
            error_emails=len(errors),
            files_generated=total_generated,
            files_failed=total_file_errors
        )
        
        # Log successful emails (consolidated single entry per email)
        for result in successful:
            generated = [f.filename for f in result.files_generated if f.error is None]
            failed = [f"{f.filename}: {f.error}" for f in result.files_generated if f.error]
            if failed:
                logger.warning(
                    "Email processed with some files failed",
                    supplier=result.email_result.supplier_name or "UNKNOWN",
                    method=result.email_result.detection_method or "unknown",
                    from_address=result.email_result.from_address,
                    original_sender=result.email_result.original_sender,
                    subject=result.email_result.subject[:60],
                    files_generated=generated,
                    files_failed=failed
                )
            else:
                logger.info(
                    "Email processed successfully",
                    supplier=result.email_result.supplier_name or "UNKNOWN",
                    method=result.email_result.detection_method or "unknown",
                    from_address=result.email_result.from_address,
                    original_sender=result.email_result.original_sender,
                    subject=result.email_result.subject[:60],
                    files_generated=generated
                )
        
        # Log file failures (emails where all files failed to parse)
        for result in file_failures:
            failed = [f"{f.filename}: {f.error}" for f in result.files_generated if f.error]
            logger.warning(
                "Email file parsing failed",
                supplier=result.email_result.supplier_name or "UNKNOWN",
                method=result.email_result.detection_method or "unknown",
                from_address=result.email_result.from_address,
                original_sender=result.email_result.original_sender,
                subject=result.email_result.subject[:60],
                files_failed=failed
            )
        
        # Log warnings
        for result in warnings:
            logger.warning(
                "Email processed with warnings",
                supplier=result.email_result.supplier_name or "UNKNOWN",
                method=result.email_result.detection_method or "unknown",
                from_address=result.email_result.from_address,
                original_sender=result.email_result.original_sender,
                subject=result.email_result.subject[:60],
                warnings=result.warnings,
                is_unknown_domain=result.email_result.is_unknown_domain
            )
        
        # Log errors
        for result in errors:
            logger.error(
                "Email processing error",
                supplier=result.email_result.supplier_name or "UNKNOWN",
                from_address=result.email_result.from_address,
                errors=result.errors
            )
        
        # Update state if not dry run and emails were actually processed
        if not args.dry_run and final_search_date and results:
            logger.info(f"Updating state with timestamp: {final_search_date.isoformat()}")
            state_manager.update_last_processed(final_search_date.isoformat())
        
        # Send summary email if requested and not dry run
        if args.send_summary and not args.dry_run and results:
            logger.info("")
            logger.info("Sending summary email...")
            email_sender = EmailSender(gmail_client=gmail_client)
            recipients = config['core'].get('notification', {}).get('summary_email_recipients', ['operations@ucalexports.com'])
            from_email = config['core'].get('notification', {}).get('summary_from_email')
            email_sender.send_summary_from_orchestrator_results(
                results=results,
                recipients=recipients,
                dry_run=False,
                from_email=from_email
            )
            logger.info("Summary email sent")
        
        # Write summary to file if requested
        if args.print_summary and results:
            logger.info("")
            logger.info(f"Writing summary to: {args.print_summary}")
            email_sender = EmailSender(gmail_client=gmail_client)
            subject, text_body = email_sender.build_summary_text_from_orchestrator_results(results)
            
            with open(args.print_summary, 'w') as f:
                f.write(f"Subject: {subject}\n")
                f.write("=" * 60 + "\n\n")
                f.write(text_body)
            
            logger.info(f"Summary written to: {args.print_summary}")
        
        logger.info("Execution complete")
        
    except Exception as e:
        logger.error(f"Failed to process emails: {str(e)}", exc_info=True)
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run email processor locally for testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with dry-run (no state updates, no emails)
  python scripts/run_email_local.py --dry-run

  # Reprocess from specific date (requires --start-date with --force)
  python scripts/run_email_local.py --force --start-date 2026-01-01 --dry-run

  # Reprocess last 30 days with higher limit (up to 50 emails)
  python scripts/run_email_local.py --force --start-date 2025-12-15 --max-emails 50 --dry-run

  # Reprocess with custom date range (overrides 7-day window)
  python scripts/run_email_local.py --force --start-date 2025-11-23 --end-date 2026-01-20 --max-emails 70

  # Run with test config
  python scripts/run_email_local.py --use-test-config --dry-run

  # Process only 5 emails from state
  python scripts/run_email_local.py --dry-run --max-emails 5

  # Full run with summary email
  python scripts/run_email_local.py --send-summary

  # Preview summary email content without sending (write to file)
  python scripts/run_email_local.py --dry-run --max-emails 10 --print-summary summary.txt

  # Full cloud emulation: use state, update state, write summary to file
  python scripts/run_email_local.py --max-emails 10 --print-summary summary.txt

  # Skip BigQuery reconciliation, upload unreconciled CSV to Drive
  python scripts/run_email_local.py --max-emails 5 --skip-bigquery

  # Skip BigQuery and dry-run (local processing only, no uploads)
  python scripts/run_email_local.py --max-emails 5 --skip-bigquery --dry-run
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Skip state updates and summary email sending'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Reprocess emails from specified start date (requires --start-date)'
    )
    
    parser.add_argument(
        '--use-test-config',
        action='store_true',
        help='Use test configurations instead of production'
    )
    
    parser.add_argument(
        '--max-emails',
        type=int,
        default=10,
        help='Maximum number of emails to process (default: 10)'
    )
    
    parser.add_argument(
        '--send-summary',
        action='store_true',
        help='Send summary email after processing (only if not dry-run)'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for processing (required with --force). Format: YYYY-MM-DD'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for processing (overrides default 7-day window). Format: YYYY-MM-DD'
    )
    
    parser.add_argument(
        '--print-summary',
        type=str,
        metavar='FILE',
        help='Write summary email content to FILE (e.g., summary.txt)'
    )
    
    parser.add_argument(
        '--skip-bigquery',
        action='store_true',
        help='Skip BigQuery upload and supersession reconciliation (upload unreconciled CSV to Drive)'
    )
    
    args = parser.parse_args()
    
    # Run
    try:
        run_email_processor(args)
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

