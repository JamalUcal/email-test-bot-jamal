"""
Web Scraping Entry Point

Handles website scraping workflow only. This provides a separate entry point
for web scraping functionality, keeping it isolated from email processing.
"""

import functions_framework
from flask import Request
import json
import os
from datetime import datetime, timezone
import traceback
import asyncio
from typing import Dict, Any, cast

from utils.logger import setup_logger, get_logger
from utils.state_manager import StateManager
from config.config_manager import ConfigManager
from gmail.gmail_client import GmailClient
from scrapers.web_scraping_orchestrator import WebScrapingOrchestrator
from notification.email_sender import EmailSender
from google.cloud import secretmanager

# Initialize logger
logger = setup_logger(__name__)


@functions_framework.http
def web_scraper(request: Request):
    """
    Web scraping entry point for Cloud Function.
    
    Args:
        request: Flask request object
        
    Returns:
        Tuple of (response_body, status_code)
    """
    execution_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Web scraper invoked", execution_id=execution_id)
    
    try:
        # Parse request for manual trigger options
        request_json = request.get_json(silent=True)
        force_execution = request_json.get('force_execution', False) if request_json else False
        dry_run = request_json.get('dry_run', False) if request_json else False
        scraper_supplier = request_json.get('scraper_supplier') if request_json else None
        
        logger.info(
            f"Web scraper parameters",
            force_execution=force_execution,
            dry_run=dry_run,
            scraper_supplier=scraper_supplier
        )
        
        # Load configuration
        logger.info("Loading configuration from GCS")
        config_manager = ConfigManager()
        config = config_manager.load_all_configs()
        
        # DEBUG: Log AUTOCAR/MERCEDES config right after loading
        for scraper in config.get('scrapers', []):
            if scraper.get('supplier') == 'AUTOCAR':
                for brand_cfg in scraper.get('config', []):
                    if brand_cfg.get('brand') == 'MERCEDES':
                        logger.info(f"DEBUG: Loaded AUTOCAR/MERCEDES from GCS")
                        break
                break
        
        # Initialize state manager
        state_manager = StateManager(
            bucket_name=config['core']['gcp']['bucket_name'],
            state_file_path=config['core']['gcp']['state_file']
        )
        
        # Check if execution should proceed
        if not force_execution:
            should_execute, reason = _should_execute_scrapers(config.get('scrapers', []), state_manager)
            if not should_execute:
                logger.info(f"Skipping execution: {reason}")
                return json.dumps({
                    'status': 'skipped',
                    'reason': reason,
                    'execution_id': execution_id
                }), 200
        
        logger.info("Starting web scraping")
        
        # Get service account credentials from Secret Manager
        # NOTE: Domain-wide delegation REQUIRES service account key (cannot use ADC)
        service_account_info = _get_service_account_credentials(
            config['core']['gcp']['project_id'],
            config['core']['gcp']['secret_name']
        )
        
        # Initialize Gmail client (needed for notifications)
        gmail_client = GmailClient(
            service_account_info=service_account_info,
            delegated_user=config['core']['gmail']['delegated_user_email']
        )
        
        # Initialize web scraping orchestrator
        web_scraping_orchestrator = WebScrapingOrchestrator(
            scraper_configs=config.get('scrapers', []),
            supplier_configs=config['suppliers'],
            brand_configs=config['brands'],
            core_config=config['core'],
            service_account_info=service_account_info,
            column_mapping_config=config['column_mapping'],
            state_manager=state_manager
        )
        
        # Pre-populate brand config cache so scrapers don't need to load from filesystem
        from scrapers.brand_matcher import set_brand_configs_cache
        set_brand_configs_cache(config['brands'])
        
        # Process scrapers
        if scraper_supplier:
            logger.info(f"Processing specific scraper supplier: {scraper_supplier}")
            
            # Find scraper config for supplier
            scraper_config = None
            for scraper in config.get('scrapers', []):
                if scraper['supplier'] == scraper_supplier:
                    scraper_config = scraper
                    break
            
            if not scraper_config:
                raise ValueError(f"No scraper configuration found for supplier: {scraper_supplier}")
            
            # Process single supplier scraper
            scraping_results = asyncio.run(
                web_scraping_orchestrator.process_scheduled_scrapers(dry_run=dry_run, force=force_execution)
            )
            
            # Filter to only the requested supplier
            scraping_results = [r for r in scraping_results if r.supplier == scraper_supplier]
        else:
            # Process all scheduled scrapers
            scraping_results = asyncio.run(
                web_scraping_orchestrator.process_scheduled_scrapers(dry_run=dry_run, force=force_execution)
            )
        
        # Convert scraping results to email processing results format for compatibility
        processing_results = []
        for scraping_result in scraping_results:
            from gmail.email_processor import EmailResult
            from orchestrator import EmailProcessingResult
            
            # Determine if all files were duplicates (scraper found files but all were duplicates)
            scraper_all_duplicates = (
                scraping_result.total_files_found > 0 and  # Found at least one file
                len(scraping_result.files_processed) == 0 and  # But processed none
                len(scraping_result.errors) == 0  # And no errors
            )
            
            mock_email_result = EmailResult(
                message_id=f"scraped_{scraping_result.supplier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                subject=f"Scraped files from {scraping_result.supplier}",
                from_address=f"scraper@{scraping_result.supplier.lower()}.com",
                from_domain=scraping_result.supplier.lower(),
                date=datetime.now(timezone.utc),
                supplier_name=scraping_result.supplier,
                is_ignored=False,
                is_unknown_domain=False,
                supported_attachments=[],
                ignored_attachments=[],
                scraper_url=scraping_result.scraper_url,
                scraper_all_duplicates=scraper_all_duplicates
            )
            
            processing_result = EmailProcessingResult(email_result=mock_email_result)
            # Convert ScrapedFileOutput to FileOutput
            from orchestrator import FileOutput
            processing_result.files_generated = [
                FileOutput(
                    filename=f.filename,
                    local_path=f.local_path,
                    drive_file_id=f.drive_file_id,
                    drive_link=f.drive_link,
                    brand=f.brand,
                    supplier=f.supplier,
                    warnings=f.warnings,
                    error=f.error,
                    total_rows=f.total_rows,
                    valid_rows=f.valid_rows,
                    parsing_errors_count=f.parsing_errors_count
                )
                for f in scraping_result.files_processed
            ]
            processing_result.errors = scraping_result.errors
            processing_result.warnings = scraping_result.warnings
            
            processing_results.append(processing_result)
        
        # Conditional summary email sending based on notification mode
        notification_config = config['core']['notification']
        summary_mode = notification_config.get('summary_mode', 'immediate')
        summary_sent = False
        
        if summary_mode == 'immediate':
            # Send email immediately (for testing)
            logger.info("Sending immediate summary email")
            email_sender = EmailSender(gmail_client)
            from_email = notification_config.get('summary_from_email')
            
            summary_sent = email_sender.send_summary_from_orchestrator_results(
                results=processing_results,
                recipients=notification_config['summary_email_recipients'],
                dry_run=dry_run,
                from_email=from_email
            )
            
        elif summary_mode == 'daily':
            # Store results for daily aggregation
            if processing_results and not dry_run:
                logger.info("Storing results for daily summary aggregation")
                state_manager.store_run_results(
                    execution_id=execution_id,
                    results=processing_results,
                    timestamp=datetime.now(timezone.utc).isoformat()
                )
            
            # Check if it's time to send daily summary
            from utils.summary_helpers import should_send_daily_summary, reconstruct_results_from_storage
            if should_send_daily_summary(config['core'], state_manager):
                logger.info("Sending daily aggregated summary email")
                pending_results = state_manager.get_pending_results()
                
                if pending_results:
                    # Reconstruct EmailProcessingResult objects from stored JSON
                    aggregated_results = reconstruct_results_from_storage(pending_results)
                    
                    email_sender = EmailSender(gmail_client)
                    from_email = notification_config.get('summary_from_email')
                    
                    summary_sent = email_sender.send_summary_from_orchestrator_results(
                        results=aggregated_results,
                        recipients=notification_config['summary_email_recipients'],
                        dry_run=dry_run,
                        from_email=from_email
                    )
                    
                    if summary_sent and not dry_run:
                        state_manager.clear_pending_results()
                        state_manager.update_last_summary_sent()
                else:
                    logger.info("No pending results to send in daily summary")
        else:
            logger.warning(f"Unknown summary_mode: {summary_mode}, defaulting to immediate")
            email_sender = EmailSender(gmail_client)
            from_email = notification_config.get('summary_from_email')
            
            summary_sent = email_sender.send_summary_from_orchestrator_results(
                results=processing_results,
                recipients=notification_config['summary_email_recipients'],
                dry_run=dry_run,
                from_email=from_email
            )
        
        # Update scraping state
        if not dry_run:
            now = datetime.now(timezone.utc).isoformat()
            for scraping_result in scraping_results:
                if scraping_result.scraping_result.success:
                    state_manager.update_last_scraped(scraping_result.supplier, now)
        
        # Calculate summary stats
        suppliers_processed = len(scraping_results)
        files_generated = sum(len(r.files_processed) for r in scraping_results)
        total_errors = sum(len(r.errors) for r in scraping_results)
        total_warnings = sum(len(r.warnings) for r in scraping_results)
        
        logger.info(
            "Web scraping completed successfully",
            suppliers_processed=suppliers_processed,
            files_generated=files_generated,
            errors=total_errors,
            warnings=total_warnings,
            summary_sent=summary_sent
        )
        
        return json.dumps({
            'status': 'success',
            'execution_id': execution_id,
            'summary': {
                'suppliers_processed': suppliers_processed,
                'files_generated': files_generated,
                'errors': total_errors,
                'warnings': total_warnings,
                'summary_sent': summary_sent
            }
        }), 200
        
    except Exception as e:
        logger.error(
            f"Web scraping failed: {str(e)}",
            error=str(e),
            traceback=traceback.format_exc()
        )
        
        return json.dumps({
            'status': 'error',
            'execution_id': execution_id,
            'error': str(e)
        }), 500


def _get_service_account_credentials(project_id: str, secret_name: str) -> dict:
    """
    Retrieve service account credentials from Secret Manager.
    
    Args:
        project_id: GCP project ID
        secret_name: Secret name in Secret Manager
        
    Returns:
        Service account credentials as dict
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        credentials_json = response.payload.data.decode('UTF-8')
        return cast(Dict[str, Any], json.loads(credentials_json))
    except Exception as e:
        logger.error(
            "Failed to retrieve service account credentials",
            error=str(e),
            project_id=project_id,
            secret_name=secret_name
        )
        raise


def _should_execute_scrapers(scraper_configs: list, state_manager: StateManager) -> tuple[bool, str]:
    """
    Determines if any scrapers should execute based on their schedules.
    
    Args:
        scraper_configs: List of scraper configurations
        state_manager: State manager instance
        
    Returns:
        Tuple of (should_execute: bool, reason: str)
    """
    try:
        if not scraper_configs:
            return False, "No scraper configurations found"
        
        # Check if any scrapers are scheduled to run
        for scraper_config in scraper_configs:
            if not scraper_config.get('enabled', False):
                continue
            
            supplier_name = scraper_config['supplier']
            schedule = scraper_config.get('schedule', {})
            frequency = schedule.get('frequency', 'weekly')
            
            # Simple scheduling logic - in production you'd want more sophisticated scheduling
            if frequency == 'daily':
                return True, f"Daily scraper {supplier_name} scheduled to run"
            elif frequency == 'weekly':
                # Check if it's the right day of the week
                day_of_week = schedule.get('day_of_week', 'monday')
                current_day_name = datetime.now().strftime('%A').lower()
                if current_day_name == day_of_week.lower():
                    return True, f"Weekly scraper {supplier_name} scheduled to run on {day_of_week}"
            elif frequency == 'monthly':
                # Check if it's the right day of the month
                day_of_month = schedule.get('day_of_month', 1)
                current_day_number = datetime.now().day
                if current_day_number == day_of_month:
                    return True, f"Monthly scraper {supplier_name} scheduled to run on day {day_of_month}"
        
        return False, "No scrapers scheduled to run at this time"
        
    except Exception as e:
        logger.error(f"Error checking scraper schedule: {str(e)}")
        # Default to executing if we can't determine schedule
        return True, "Error checking schedule, defaulting to execute"


# For local testing
if __name__ == "__main__":
    from flask import Flask
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def test_handler():
        from flask import request
        return web_scraper(request)
    
    app.run(host='0.0.0.0', port=8081, debug=True)
