#!/usr/bin/env python3
"""
Local web scraper runner for development and testing.

Allows running scrapers locally with various options including:
- Dry run mode (skip Drive upload)
- Force execution (ignore schedule)
- Test config (use brand_config_test.json)
- Timeout simulation
- Single supplier or all enabled
"""

import sys
import os
import asyncio
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scrapers.web_scraping_orchestrator import WebScrapingOrchestrator
from scrapers.execution_monitor import ExecutionMonitor
from scrapers.schedule_evaluator import ScheduleEvaluator
from utils.state_manager import StateManager
from utils.logger import setup_logger
from datetime import datetime, timezone
import json

logger = setup_logger(__name__)


def load_local_configs(use_test_config: bool = False):
    """
    Load configuration files from local filesystem.
    
    Args:
        use_test_config: If True, load brand_config_test.json
        
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
    
    # Log sample of brands with drive folder IDs for verification
    sample_brands = [
        (b['brand'], b.get('driveFolderId', 'MISSING'))
        for b in brand_config[:5]
        if b.get('driveFolderId')
    ]
    logger.info(
        f"Loaded {len(brand_config)} brands from {brand_filename}",
        sample_brands_with_folders=sample_brands
    )
    
    # Load scraper config
    with open(config_dir / "scraper" / "scraper_config.json") as f:
        scraper_config = json.load(f)
    
    # Load column mapping config
    with open(config_dir / "core" / "column_mapping_config.json") as f:
        column_mapping_config = json.load(f)
    
    # Load currency config
    with open(config_dir / "core" / "currency_config.json") as f:
        currency_config = json.load(f)
    
    config_mode = "TEST" if use_test_config else "PRODUCTION"
    logger.info(f"Loaded configs in {config_mode} mode")
    
    return {
        'core': core_config,
        'suppliers': supplier_config,
        'brands': brand_config,
        'scrapers': scraper_config,
        'column_mapping': column_mapping_config,
        'currency': currency_config
    }


async def run_scraper(args):
    """
    Run web scraper with specified options.
    
    Args:
        args: Parsed command line arguments
    """
    logger.info("LOCAL WEB SCRAPER")
    logger.info(f"Supplier: {args.supplier or 'ALL ENABLED'}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Skip BigQuery: {args.skip_bigquery}")
    logger.info(f"Force execution: {args.force}")
    logger.info(f"Test config: {args.use_test_config}")
    logger.info(f"Max execution time: {args.max_execution_time}s")
    
    # Load configurations
    config = load_local_configs(use_test_config=args.use_test_config)
    
    # Set environment variable for brand_matcher to use test config
    if args.use_test_config:
        os.environ['USE_TEST_BRAND_CONFIG'] = 'true'
    
    # Initialize state manager (local mode)
    state_manager = StateManager(
        bucket_name="dummy",
        state_file_path="scraper_state.json",
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
        logger.warning(
            "No service account credentials found. "
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable or use --dry-run mode. "
            "Falling back to Application Default Credentials (ADC)."
        )
    
    # Initialize orchestrator
    orchestrator = WebScrapingOrchestrator(
        scraper_configs=config['scrapers'],
        supplier_configs=config['suppliers'],
        brand_configs=config['brands'],
        core_config=config['core'],
        service_account_info=service_account_info,
        column_mapping_config=config['column_mapping'],
        currency_config=config['currency'],
        state_manager=state_manager
    )
    
    # Pre-populate brand config cache so scrapers don't need to load from filesystem
    from scrapers.brand_matcher import set_brand_configs_cache
    set_brand_configs_cache(config['brands'])
    
    # Create execution monitor
    monitor = ExecutionMonitor(
        max_duration_seconds=args.max_execution_time,
        buffer_seconds=60
    )
    
    # Create schedule evaluator
    schedule_evaluator = ScheduleEvaluator()
    
    # Get list of scrapers to process
    scrapers_to_process = []
    
    if args.supplier:
        # Process specific supplier
        for scraper_config in config['scrapers']:
            if scraper_config['supplier'] == args.supplier:
                scrapers_to_process = [scraper_config]
                break
        
        if not scrapers_to_process:
            logger.error(f"Supplier '{args.supplier}' not found in config")
            sys.exit(1)
    else:
        # Process all enabled scrapers
        scrapers_to_process = [
            s for s in config['scrapers']
            if s.get('enabled', False)
        ]
    
    logger.info(f"Processing {len(scrapers_to_process)} scraper(s)")
    
    # Process each scraper
    results = []
    
    for scraper_config in scrapers_to_process:
        # Check global timeout
        if monitor.should_stop():
            logger.warning("Global timeout approaching - stopping")
            break
        
        supplier_name = scraper_config['supplier']
        
        # Check if scheduled to run
        supplier_state = state_manager.get_supplier_state(supplier_name)
        should_run = schedule_evaluator.should_run_scraper(
            scraper_config=scraper_config,
            supplier_state=supplier_state,
            current_time=datetime.now(timezone.utc),
            force=args.force
        )
        
        if not should_run and not args.force:
            logger.info(f"Skipping {supplier_name} - not scheduled to run")
            continue
        
        # Create supplier-specific monitor
        supplier_timeout = scraper_config.get('execution', {}).get('max_execution_time_seconds', 600)
        supplier_buffer = scraper_config.get('execution', {}).get('timeout_buffer_seconds', 60)
        
        supplier_monitor = ExecutionMonitor(
            max_duration_seconds=supplier_timeout,
            buffer_seconds=supplier_buffer
        )
        
        logger.info(f"Running scraper: {supplier_name}")
        
        try:
            result = await orchestrator.run_scraper_with_timeout(
                scraper_config=scraper_config,
                monitor=supplier_monitor,
                dry_run=args.dry_run,
                skip_bigquery=args.skip_bigquery
            )
            
            results.append(result)
            
            # Log results
            logger.info(f"RESULTS: {supplier_name}")
            logger.info(f"Success: {result.scraping_result.success}")
            logger.info(f"Files processed: {len(result.files_processed)}")
            logger.info(f"Warnings: {len(result.warnings)}")
            logger.info(f"Errors: {len(result.errors)}")
            
            if result.files_processed:
                logger.info("\nProcessed files:")
                for file_output in result.files_processed:
                    status = "✓" if not file_output.error else "✗"
                    logger.info(f"  {status} {file_output.filename}")
                    if file_output.drive_link:
                        logger.info(f"    Drive: {file_output.drive_link}")
            
            if result.warnings:
                logger.warning("\nWarnings:")
                for warning in result.warnings:
                    logger.warning(f"  - {warning}")
            
            if result.errors:
                logger.error("\nErrors:")
                for error in result.errors:
                    logger.error(f"  - {error}")
            
        except Exception as e:
            logger.error(f"Failed to process {supplier_name}: {str(e)}", exc_info=True)
    
    # Final summary
    logger.info("EXECUTION SUMMARY")
    logger.info(f"Scrapers processed: {len(results)}")
    logger.info(f"Total execution time: {round(monitor.elapsed_time(), 2)}s")
    
    success_count = sum(1 for r in results if r.scraping_result.success)
    logger.info(f"Successful: {success_count}/{len(results)}")
    
    total_files = sum(len(r.files_processed) for r in results)
    logger.info(f"Total files: {total_files}")
    logger.info(f"{'='*80}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run web scrapers locally for testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all enabled scrapers (respects schedule)
  python scripts/run_scraper_local.py

  # Run specific supplier with dry-run
  python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

  # Force execution with test Drive folders
  python scripts/run_scraper_local.py --supplier APF --force --use-test-config

  # Test timeout handling
  python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run --max-execution-time 30

  # Run all with test config
  python scripts/run_scraper_local.py --use-test-config --dry-run

  # Skip BigQuery reconciliation, upload unreconciled CSV to Drive
  python scripts/run_scraper_local.py --supplier NEOPARTA --skip-bigquery

  # Skip BigQuery and dry-run (local processing only, no uploads)
  python scripts/run_scraper_local.py --supplier NEOPARTA --skip-bigquery --dry-run
        """
    )
    
    parser.add_argument(
        '--supplier',
        help='Process specific supplier (e.g., NEOPARTA, APF)',
        default=None
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Skip Drive upload (only process locally)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Ignore schedule and run immediately'
    )
    
    parser.add_argument(
        '--use-test-config',
        action='store_true',
        help='Use brand_config_test.json instead of brand_config.json'
    )
    
    parser.add_argument(
        '--max-execution-time',
        type=int,
        default=3600,
        help='Maximum execution time in seconds (default: 3600)'
    )
    
    parser.add_argument(
        '--screenshots',
        action='store_true',
        help='Enable browser screenshots for debugging (default: disabled)'
    )
    
    parser.add_argument(
        '--skip-bigquery',
        action='store_true',
        help='Skip BigQuery upload and supersession reconciliation (upload unreconciled CSV to Drive)'
    )
    
    args = parser.parse_args()
    
    # Set screenshot environment variable
    if args.screenshots:
        os.environ['ENABLE_SCREENSHOTS'] = 'true'
        logger.info("Screenshots enabled for debugging")
    else:
        os.environ['ENABLE_SCREENSHOTS'] = 'false'
    
    # Run async
    try:
        asyncio.run(run_scraper(args))
    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
