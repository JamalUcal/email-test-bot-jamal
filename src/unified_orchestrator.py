"""
Unified Orchestrator - coordinates both email and web scraping workflows.

This provides a single entry point that can handle both email processing
and web scraping, with proper separation of concerns and scheduling.
"""

import functions_framework
from flask import Request
import json
import os
from datetime import datetime, timezone
import traceback
import asyncio
from typing import Dict, Any, Optional, cast

from utils.logger import setup_logger, get_logger
from utils.state_manager import StateManager
from config.config_manager import ConfigManager
from gmail.gmail_client import GmailClient
from orchestrator import ProcessingOrchestrator
from scrapers.web_scraping_orchestrator import WebScrapingOrchestrator
from notification.email_sender import EmailSender
from google.cloud import secretmanager

# Initialize logger
logger = setup_logger(__name__)


@functions_framework.http
def unified_orchestrator(request: Request):
    """
    Unified orchestrator entry point for Cloud Function.
    
    Coordinates both email processing and web scraping based on configuration
    and scheduling requirements.
    
    Args:
        request: Flask request object
        
    Returns:
        Tuple of (response_body, status_code)
    """
    execution_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Unified orchestrator invoked", execution_id=execution_id)
    
    try:
        # Parse request for manual trigger options
        request_json = request.get_json(silent=True)
        force_execution = request_json.get('force_execution', False) if request_json else False
        dry_run = request_json.get('dry_run', False) if request_json else False
        email_id = request_json.get('email_id') if request_json else None
        scraper_supplier = request_json.get('scraper_supplier') if request_json else None
        workflow = request_json.get('workflow', 'auto') if request_json else 'auto'  # 'email', 'scraping', 'auto'
        
        logger.info(
            f"Unified orchestrator parameters",
            force_execution=force_execution,
            dry_run=dry_run,
            email_id=email_id,
            scraper_supplier=scraper_supplier,
            workflow=workflow
        )
        
        # Load configuration
        logger.info("Loading configuration from GCS")
        config_manager = ConfigManager()
        config = config_manager.load_all_configs()
        
        # Initialize state manager
        state_manager = StateManager(
            bucket_name=config['core']['gcp']['bucket_name'],
            state_file_path=config['core']['gcp']['state_file']
        )
        
        # Get service account credentials from Secret Manager
        # NOTE: Domain-wide delegation REQUIRES service account key (cannot use ADC)
        service_account_info = _get_service_account_credentials(
            config['core']['gcp']['project_id'],
            config['core']['gcp']['secret_name']
        )
        
        # Initialize Gmail client
        gmail_client = GmailClient(
            service_account_info=service_account_info,
            delegated_user=config['core']['gmail']['delegated_user_email']
        )
        
        # Initialize orchestrators
        email_orchestrator = ProcessingOrchestrator(
            gmail_client=gmail_client,
            supplier_configs=config['suppliers'],
            brand_configs=config['brands'],
            core_config=config['core'],
            service_account_info=service_account_info,
            column_mapping_config=config['column_mapping'],
            currency_config=config['currency']
        )
        
        web_scraping_orchestrator = WebScrapingOrchestrator(
            scraper_configs=config.get('scrapers', []),
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
        
        # Determine what to execute
        # Execute web scraping FIRST, then email processing
        email_results = []
        scraping_results = []
        
        # Run web scraping before email processing
        run_scraping = workflow == 'scraping' or (workflow == 'auto' and _should_run_web_scraping(config.get('scrapers', []), state_manager, force_execution))
        run_email = workflow == 'email' or (workflow == 'auto' and _should_run_email_processing(config['core'], state_manager, force_execution))
        
        if run_scraping:
            logger.info("Executing web scraping workflow")
            scraping_results = _execute_web_scraping(
                web_scraping_orchestrator=web_scraping_orchestrator,
                config=config,
                state_manager=state_manager,
                scraper_supplier=scraper_supplier,
                dry_run=dry_run,
                force_execution=force_execution
            )
        
        if run_email:
            logger.info("Executing email processing workflow")
            email_results = _execute_email_processing(
                email_orchestrator=email_orchestrator,
                gmail_client=gmail_client,
                config=config,
                state_manager=state_manager,
                email_id=email_id,
                dry_run=dry_run,
                force_execution=force_execution
            )
        
        # Combine results
        all_results = scraping_results + email_results
        
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
                results=all_results,
                recipients=notification_config['summary_email_recipients'],
                dry_run=dry_run,
                from_email=from_email
            )
            
        elif summary_mode == 'daily':
            # Store results for daily aggregation
            if all_results and not dry_run:
                logger.info("Storing results for daily summary aggregation")
                state_manager.store_run_results(
                    execution_id=execution_id,
                    results=all_results,
                    timestamp=datetime.now(timezone.utc).isoformat()
                )
            
            # Check if it's time to send daily summary
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
                results=all_results,
                recipients=notification_config['summary_email_recipients'],
                dry_run=dry_run,
                from_email=from_email
            )
        
        # Clean up
        email_orchestrator.cleanup()
        
        # Update state
        if not dry_run:
            state_manager.update_last_execution()
            
            # Update email processing state
            if email_results:
                now = datetime.now(timezone.utc)
                if email_results[-1].email_result.date:
                    last_email_date = email_results[-1].email_result.date
                    if last_email_date > now:
                        last_email_date = now
                    state_manager.update_last_processed(last_email_date.isoformat())
            
            # Update scraping state
            for scraping_result in scraping_results:
                if scraping_result.scraping_result.success:
                    now_str = datetime.now(timezone.utc).isoformat()
                    state_manager.update_last_scraped(scraping_result.supplier, now_str)
        
        # Calculate summary stats
        emails_processed = len(email_results)
        suppliers_scraped = len(scraping_results)
        files_generated = sum(len(r.files_generated) for r in all_results)
        total_errors = sum(len(r.errors) for r in all_results)
        total_warnings = sum(len(r.warnings) for r in all_results)
        
        logger.info(
            "Unified orchestrator completed successfully",
            emails_processed=emails_processed,
            suppliers_scraped=suppliers_scraped,
            files_generated=files_generated,
            errors=total_errors,
            warnings=total_warnings,
            summary_sent=summary_sent
        )
        
        return json.dumps({
            'status': 'success',
            'execution_id': execution_id,
            'summary': {
                'emails_processed': emails_processed,
                'suppliers_scraped': suppliers_scraped,
                'files_generated': files_generated,
                'errors': total_errors,
                'warnings': total_warnings,
                'summary_sent': summary_sent
            }
        }), 200
        
    except Exception as e:
        logger.error(
            f"Unified orchestrator failed: {str(e)}",
            error=str(e),
            traceback=traceback.format_exc()
        )
        
        return json.dumps({
            'status': 'error',
            'execution_id': execution_id,
            'error': str(e)
        }), 500


def _execute_email_processing(
    email_orchestrator: ProcessingOrchestrator,
    gmail_client: GmailClient,
    config: dict,
    state_manager: StateManager,
    email_id: Optional[str],
    dry_run: bool,
    force_execution: bool
) -> list:
    """Execute email processing workflow."""
    from orchestrator import EmailProcessingResult
    
    if email_id:
        logger.info(f"Processing specific email ID: {email_id}")
        message = gmail_client.get_message(email_id)
        result = email_orchestrator.process_email(message, dry_run=dry_run)
        return [result]
    else:
        # Get last processed timestamp
        state = state_manager.get_state()
        last_processed_str = state.get('last_processed_timestamp')
        after_date = None
        if last_processed_str:
            after_date = datetime.fromisoformat(
                last_processed_str.replace('Z', '+00:00')
            )
            logger.info(
                f"Starting from last processed date: {after_date.isoformat()}",
                last_processed_timestamp=after_date.isoformat()
            )
        else:
            logger.info("No previous state found, processing all emails")
        
        # Process emails
        max_emails = config['core']['gmail'].get('max_emails_per_run', 100)
        logger.info(
            f"Processing up to {max_emails} emails after {after_date.isoformat() if after_date else 'beginning'}",
            after_date=after_date.isoformat() if after_date else None,
            max_emails=max_emails
        )
        processing_results, final_search_date = email_orchestrator.process_emails(
            after_date=after_date,
            max_emails=max_emails,
            dry_run=dry_run
        )
        
        return processing_results


def _execute_web_scraping(
    web_scraping_orchestrator: WebScrapingOrchestrator,
    config: dict,
    state_manager: StateManager,
    scraper_supplier: Optional[str],
    dry_run: bool,
    force_execution: bool
) -> list:
    """
    Execute web scraping workflow with timeout management.
    
    Args:
        web_scraping_orchestrator: Web scraping orchestrator instance
        config: Complete configuration dictionary
        state_manager: State manager for tracking progress
        scraper_supplier: Optional specific supplier to process
        dry_run: If True, skip Drive upload
        force_execution: If True, ignore schedule and run immediately
        
    Returns:
        List of EmailProcessingResult objects (for compatibility with unified reporting)
    """
    from orchestrator import EmailProcessingResult, FileOutput
    from gmail.email_processor import EmailResult
    from scrapers.execution_monitor import ExecutionMonitor
    from scrapers.schedule_evaluator import ScheduleEvaluator
    
    # Create global timeout monitor for Cloud Function execution
    # Cloud Function 2nd gen has 60 minute timeout, use buffer
    global_timeout_seconds = 3600  # 60 minutes
    global_buffer_seconds = 180     # 3 minutes buffer
    
    global_monitor = ExecutionMonitor(
        max_duration_seconds=global_timeout_seconds,
        buffer_seconds=global_buffer_seconds
    )
    
    logger.info(
        "Starting web scraping workflow with timeout management",
        global_timeout_seconds=global_timeout_seconds,
        global_buffer_seconds=global_buffer_seconds
    )
    
    schedule_evaluator = ScheduleEvaluator()
    scraping_results = []
    
    # Get list of scrapers to process
    scrapers_to_process = []
    if scraper_supplier:
        # Process specific supplier
        logger.info(f"Processing specific scraper supplier: {scraper_supplier}")
        for scraper_config in config.get('scrapers', []):
            if scraper_config['supplier'] == scraper_supplier:
                scrapers_to_process = [scraper_config]
                break
        
        if not scrapers_to_process:
            raise ValueError(f"No scraper configuration found for supplier: {scraper_supplier}")
    else:
        # Process all enabled scrapers
        scrapers_to_process = [
            s for s in config.get('scrapers', [])
            if s.get('enabled', False)
        ]
    
    # Process each scraper with individual timeout monitoring
    for scraper_config in scrapers_to_process:
        # Check global timeout
        if global_monitor.should_stop():
            logger.warning("Global timeout approaching - stopping scraper evaluation")
            break
        
        supplier_name = scraper_config['supplier']
        
        # Check if scraper is scheduled to run
        supplier_state = state_manager.get_supplier_state(supplier_name)
        should_run = schedule_evaluator.should_run_scraper(
            scraper_config=scraper_config,
            supplier_state=supplier_state,
            current_time=datetime.now(timezone.utc),
            force=force_execution
        )
        
        if not should_run and not force_execution:
            logger.info(f"Skipping {supplier_name} - not scheduled to run")
            continue
        
        # Create supplier-specific timeout monitor
        execution_config = scraper_config.get('execution', {})
        supplier_timeout = execution_config.get('max_execution_time_seconds', 600)  # 10 min default
        supplier_buffer = execution_config.get('timeout_buffer_seconds', 60)        # 1 min buffer
        
        supplier_monitor = ExecutionMonitor(
            max_duration_seconds=supplier_timeout,
            buffer_seconds=supplier_buffer
        )
        
        logger.info(
            f"Processing scraper for {supplier_name}",
            max_execution_seconds=supplier_timeout,
            buffer_seconds=supplier_buffer
        )
        
        # Run scraper with timeout monitoring
        try:
            result = asyncio.run(
                web_scraping_orchestrator.run_scraper_with_timeout(
                    scraper_config=scraper_config,
                    monitor=supplier_monitor,
                    dry_run=dry_run
                )
            )
            scraping_results.append(result)
            
            logger.info(
                f"Completed scraper for {supplier_name}",
                files_processed=len(result.files_processed),
                warnings=len(result.warnings),
                errors=len(result.errors)
            )
            
        except Exception as e:
            logger.error(f"Failed to process scraper for {supplier_name}: {str(e)}")
            # Continue with next scraper rather than failing completely
    
    logger.info(
        f"Web scraping workflow completed",
        suppliers_processed=len(scraping_results),
        elapsed_seconds=round(global_monitor.elapsed_time(), 2)
    )
    
    # Convert scraping results to email processing results format for compatibility
    processing_results = []
    for scraping_result in scraping_results:
        # Determine if all files were duplicates (scraper found files but all were duplicates)
        scraper_all_duplicates = (
            scraping_result.total_files_found > 0 and  # Found at least one file
            len(scraping_result.files_processed) == 0 and  # But processed none
            len(scraping_result.errors) == 0  # And no errors
        )
        
        mock_email_result = EmailResult(
            message_id=f"scraped_{scraping_result.supplier}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            from_address=f"scraper@{scraping_result.supplier.lower()}.com",
            from_domain=scraping_result.supplier.lower(),
            subject=f"Scraped files from {scraping_result.supplier}",
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
        processing_result.files_generated = [
            FileOutput(
                filename=file.filename,
                local_path=file.local_path,
                drive_file_id=file.drive_file_id,
                drive_link=file.drive_link,
                brand=file.brand,
                supplier=file.supplier,
                warnings=file.warnings,
                error=file.error,
                total_rows=file.total_rows,
                valid_rows=file.valid_rows,
                parsing_errors_count=file.parsing_errors_count
            )
            for file in scraping_result.files_processed
        ]
        processing_result.errors = scraping_result.errors
        processing_result.warnings = scraping_result.warnings
        
        processing_results.append(processing_result)
    
    return processing_results


def _should_run_email_processing(core_config: dict, state_manager: StateManager, force_execution: bool) -> bool:
    """
    Check if email processing should run.
    
    Email processing now runs hourly (multi-pass) to stay within Cloud Function timeouts.
    Always returns True to enable multi-pass execution.
    """
    # Email processing runs on every invocation (hourly multi-pass)
    return True


# Import shared helper functions
from utils.summary_helpers import should_send_daily_summary, reconstruct_results_from_storage


def _should_run_web_scraping(scraper_configs: list, state_manager: StateManager, force_execution: bool) -> bool:
    """Check if web scraping should run."""
    if force_execution:
        return True
    
    try:
        if not scraper_configs:
            return False
        
        # Check if any scrapers are scheduled to run
        for scraper_config in scraper_configs:
            if not scraper_config.get('enabled', False):
                continue
            
            schedule = scraper_config.get('schedule', {})
            frequency = schedule.get('frequency', 'weekly')
            
            # Simple scheduling logic
            if frequency == 'daily':
                return True
            elif frequency == 'weekly':
                day_of_week = schedule.get('day_of_week', 'monday')
                current_day_name = datetime.now().strftime('%A').lower()
                if current_day_name == day_of_week.lower():
                    return True
            elif frequency == 'monthly':
                day_of_month = int(schedule.get('day_of_month', 1))
                current_day_number = datetime.now().day
                if current_day_number == day_of_month:
                    return True
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking scraper schedule: {str(e)}")
        return True


def _get_service_account_credentials(project_id: str, secret_name: str) -> dict:
    """Retrieve service account credentials from Secret Manager."""
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


# For local testing
if __name__ == "__main__":
    from flask import Flask
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def test_handler():
        from flask import request
        return unified_orchestrator(request)
    
    app.run(host='0.0.0.0', port=8082, debug=True)
