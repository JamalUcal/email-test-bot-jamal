"""
Email Processing Entry Point

Handles email processing workflow only. This keeps the original email bot
functionality separate from the new web scraping capabilities.
"""

import functions_framework
from flask import Request
import json
from datetime import datetime, timezone
import traceback
from typing import Dict, Any, cast

from utils.logger import setup_logger, get_logger
from utils.state_manager import StateManager
from config.config_manager import ConfigManager
from gmail.gmail_client import GmailClient
from orchestrator import ProcessingOrchestrator
from notification.email_sender import EmailSender
from google.cloud import secretmanager

# Initialize logger
logger = setup_logger(__name__)


@functions_framework.http
def email_processor(request: Request):
    """
    Email processing entry point for Cloud Function.
    
    Args:
        request: Flask request object
        
    Returns:
        Tuple of (response_body, status_code)
    """
    execution_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Email processor invoked", execution_id=execution_id)
    
    try:
        # Parse request for manual trigger options
        request_json = request.get_json(silent=True)
        force_execution = request_json.get('force_execution', False) if request_json else False
        dry_run = request_json.get('dry_run', False) if request_json else False
        email_id = request_json.get('email_id') if request_json else None
        
        logger.info(
            f"Email processor parameters",
            force_execution=force_execution,
            dry_run=dry_run,
            email_id=email_id
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
        
        # Check if execution should proceed
        if not force_execution:
            should_execute, reason = _should_execute(config['core'], state_manager)
            if not should_execute:
                logger.info(f"Skipping execution: {reason}")
                return json.dumps({
                    'status': 'skipped',
                    'reason': reason,
                    'execution_id': execution_id
                }), 200
        
        logger.info("Starting email processing")
        
        # Get service account credentials from Secret Manager
        service_account_info = _get_service_account_credentials(
            config['core']['gcp']['project_id'],
            config['core']['gcp']['secret_name']
        )
        
        # Initialize Gmail client
        gmail_client = GmailClient(
            service_account_info=service_account_info,
            delegated_user=config['core']['gmail']['delegated_user_email']
        )
        
        # Initialize processing orchestrator
        orchestrator = ProcessingOrchestrator(
            gmail_client=gmail_client,
            supplier_configs=config['suppliers'],
            brand_configs=config['brands'],
            core_config=config['core'],
            service_account_info=service_account_info,
            column_mapping_config=config['column_mapping'],
            currency_config=config['currency']
        )
        
        # Process emails
        if email_id:
            logger.info(f"Processing specific email ID: {email_id}")
            message = gmail_client.get_message(email_id)
            result = orchestrator.process_email(message, dry_run=dry_run)
            processing_results = [result]
            final_search_date = None
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
            processing_results, final_search_date = orchestrator.process_emails(
                after_date=after_date,
                max_emails=max_emails,
                dry_run=dry_run
            )
        
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
                execution_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
        
        # Clean up
        orchestrator.cleanup()
        
        # Update state
        if not dry_run:
            state_manager.update_last_execution()
            
            # Get current time (never set state to future date)
            now = datetime.now(timezone.utc)
            
            # Determine what date to save
            if processing_results and processing_results[-1].email_result.date:
                # If we processed emails, use the latest email's date
                last_email_date = processing_results[-1].email_result.date
                
                # Ensure we don't set a future date
                if last_email_date > now:
                    logger.warning(
                        f"⚠️ Latest email date {last_email_date.isoformat()} is in the future, using current time instead",
                        email_date=last_email_date.isoformat(),
                        current_time=now.isoformat()
                    )
                    last_email_date = now
                
                last_email_date_str = last_email_date.isoformat()
                state_manager.update_last_processed(last_email_date_str)
                logger.info(
                    f"📅 Updated last processed timestamp to: {last_email_date_str} (from latest email)",
                    last_processed_timestamp=last_email_date_str
                )
            elif final_search_date:
                # If no emails found but we searched, use the final search date
                # But cap it at current time (don't set future date)
                if final_search_date > now:
                    logger.info(
                        f"📅 Search window extends to future ({final_search_date.isoformat()}), capping at current time",
                        search_date=final_search_date.isoformat(),
                        current_time=now.isoformat()
                    )
                    final_search_date = now
                
                final_search_date_str = final_search_date.isoformat()
                state_manager.update_last_processed(final_search_date_str)
                logger.info(
                    f"📅 Updated last processed timestamp to: {final_search_date_str} (no emails found, advanced search window)",
                    last_processed_timestamp=final_search_date_str
                )
            else:
                logger.info("No emails processed and no search performed, state not updated")
        
        # Calculate summary stats
        emails_processed = len(processing_results)
        files_generated = sum(len(r.files_generated) for r in processing_results)
        total_errors = sum(len(r.errors) for r in processing_results)
        total_warnings = sum(len(r.warnings) for r in processing_results)
        
        logger.info(
            "Email processing completed successfully",
            emails_processed=emails_processed,
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
                'files_generated': files_generated,
                'errors': total_errors,
                'warnings': total_warnings,
                'summary_sent': summary_sent
            }
        }), 200
        
    except Exception as e:
        logger.error(
            f"Email processing failed: {str(e)}",
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


def _should_execute(core_config: dict, state_manager: StateManager) -> tuple[bool, str]:
    """
    Determines if the function should execute.
    
    Email processing now runs hourly (multi-pass) to stay within Cloud Function timeouts.
    Always returns True to enable multi-pass execution.
    
    Args:
        core_config: Core configuration dictionary
        state_manager: State manager instance
        
    Returns:
        Tuple of (should_execute: bool, reason: str)
    """
    # Email processing runs on every invocation (hourly multi-pass)
    return True, "Hourly multi-pass execution enabled"


# For local testing
if __name__ == "__main__":
    from flask import Flask
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def test_handler():
        from flask import request
        return email_processor(request)
    
    app.run(host='0.0.0.0', port=8080, debug=True)
