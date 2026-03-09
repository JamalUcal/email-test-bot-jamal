"""
Helper functions for summary email management.

Shared between unified_orchestrator, email_processor, and web_scraper.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from utils.logger import get_logger
from utils.state_manager import StateManager

logger = get_logger(__name__)


def should_send_daily_summary(core_config: dict, state_manager: StateManager) -> bool:
    """
    Check if it's time to send the daily summary email.
    
    Args:
        core_config: Core configuration dictionary
        state_manager: State manager instance
        
    Returns:
        True if daily summary should be sent now
    """
    try:
        notification = core_config['notification']
        summary_hour = notification.get('daily_summary_hour', 19)
        summary_minute = notification.get('daily_summary_minute', 0)
        
        import pytz
        tz = pytz.timezone(core_config['execution']['timezone'])
        now = datetime.now(tz)
        
        # Check if we're in the time window (5-minute tolerance)
        time_diff_minutes = abs(
            (now.hour * 60 + now.minute) - 
            (summary_hour * 60 + summary_minute)
        )
        
        if time_diff_minutes > 5:
            return False
        
        # Check if we already sent today
        last_sent_str = state_manager.get_last_summary_sent_timestamp()
        if last_sent_str:
            last_sent = datetime.fromisoformat(
                last_sent_str.replace('Z', '+00:00')
            ).astimezone(tz)
            if now.date() == last_sent.date():
                logger.debug("Daily summary already sent today")
                return False  # Already sent today
        
        logger.info("Time to send daily summary", hour=now.hour, minute=now.minute)
        return True
        
    except Exception as e:
        logger.error(f"Error checking daily summary schedule: {str(e)}")
        return False


def reconstruct_results_from_storage(pending_results: list) -> list:
    """
    Reconstruct EmailProcessingResult objects from stored JSON.
    
    Args:
        pending_results: List of result entry dictionaries from state
        
    Returns:
        List of EmailProcessingResult objects
    """
    # Import here to avoid circular dependencies
    from orchestrator import EmailProcessingResult, FileOutput
    from gmail.email_processor import EmailResult
    
    def parse_datetime(dt_str: Any) -> datetime:
        """Parse datetime string, return current UTC time if None."""
        if dt_str is None:
            return datetime.now(timezone.utc)
        if isinstance(dt_str, datetime):
            return dt_str
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except Exception:
            return datetime.now(timezone.utc)
    
    reconstructed = []
    
    for result_entry in pending_results:
        try:
            results_json = result_entry['results_json']
            results_dicts = json.loads(results_json)
            
            for result_dict in results_dicts:
                # Reconstruct EmailResult
                email_result_dict = result_dict.get('email_result', {})
                # Handle legacy field name (from_email) for backwards compatibility
                from_address = email_result_dict.get('from_address') or email_result_dict.get('from_email', '')
                from_domain = email_result_dict.get('from_domain', '')
                
                email_result = EmailResult(
                    message_id=email_result_dict.get('message_id', ''),
                    subject=email_result_dict.get('subject', ''),
                    from_address=from_address,
                    from_domain=from_domain,
                    date=parse_datetime(email_result_dict.get('date')),
                    supplier_name=email_result_dict.get('supplier_name'),
                    attachments=email_result_dict.get('attachments', []),
                    is_unknown_domain=email_result_dict.get('is_unknown_domain', False),
                    parsing_errors=email_result_dict.get('parsing_errors', [])
                )
                
                # Reconstruct FileOutput objects
                files_generated = []
                for file_dict in result_dict.get('files_generated', []):
                    file_output = FileOutput(
                        filename=file_dict.get('filename', ''),
                        local_path=file_dict.get('local_path', ''),
                        drive_file_id=file_dict.get('drive_file_id'),
                        drive_link=file_dict.get('drive_link'),
                        brand=file_dict.get('brand'),
                        supplier=file_dict.get('supplier'),
                        warnings=file_dict.get('warnings', []),
                        error=file_dict.get('error'),
                        total_rows=file_dict.get('total_rows', 0),
                        valid_rows=file_dict.get('valid_rows', 0),
                        parsing_errors_count=file_dict.get('parsing_errors_count', 0)
                    )
                    files_generated.append(file_output)
                
                # Reconstruct EmailProcessingResult
                processing_result = EmailProcessingResult(
                    email_result=email_result,
                    brand_detected=result_dict.get('brand_detected'),
                    brand_source=result_dict.get('brand_source'),
                    brand_fallback_used=result_dict.get('brand_fallback_used', False),
                    expiry_date=parse_datetime(result_dict.get('expiry_date')),
                    expiry_source=result_dict.get('expiry_source'),
                    expiry_is_past=result_dict.get('expiry_is_past', False),
                    valid_from_date=parse_datetime(result_dict.get('valid_from_date')),
                    files_generated=files_generated,
                    warnings=result_dict.get('warnings', []),
                    errors=result_dict.get('errors', [])
                )
                
                reconstructed.append(processing_result)
                
        except Exception as e:
            logger.error(
                f"Failed to reconstruct result from storage: {str(e)}",
                execution_id=result_entry.get('execution_id')
            )
            # Continue with other results
            continue
    
    logger.info(
        f"Reconstructed {len(reconstructed)} results from {len(pending_results)} stored entries"
    )
    return reconstructed


