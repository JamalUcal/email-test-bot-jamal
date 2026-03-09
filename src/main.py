"""
Main Entry Point - routes requests to appropriate handlers.

This is the main entry point that can route to different workflows:
- Email processing only
- Web scraping only  
- Unified orchestrator (both)
"""

import functions_framework
from flask import Request
import json
from datetime import datetime, timezone

from utils.logger import setup_logger, get_logger
from email_processor import email_processor
from web_scraper import web_scraper
from unified_orchestrator import unified_orchestrator

# Initialize logger
logger = setup_logger(__name__)


@functions_framework.http
def main(request: Request):
    """
    Main entry point that routes to appropriate handler.
    
    Args:
        request: Flask request object
        
    Returns:
        Tuple of (response_body, status_code)
    """
    execution_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Main router invoked", execution_id=execution_id)
    
    try:
        # Parse request to determine routing
        request_json = request.get_json(silent=True)
        workflow = request_json.get('workflow', 'unified') if request_json else 'unified'
        
        logger.info(f"Routing to workflow: {workflow}")
        
        # Route to appropriate handler
        if workflow == 'email':
            return email_processor(request)
        elif workflow == 'scraping':
            return web_scraper(request)
        elif workflow == 'unified':
            return unified_orchestrator(request)
        else:
            return json.dumps({
                'status': 'error',
                'execution_id': execution_id,
                'error': f"Unknown workflow: {workflow}. Valid options: 'email', 'scraping', 'unified'"
            }), 400
            
    except Exception as e:
        logger.error(f"Main router failed: {str(e)}")
        return json.dumps({
            'status': 'error',
            'execution_id': execution_id,
            'error': str(e)
        }), 500


# For local testing
if __name__ == "__main__":
    from flask import Flask
    app = Flask(__name__)
    
    @app.route('/', methods=['POST', 'GET'])
    def test_handler():
        from flask import request
        return main(request)
    
    app.run(host='0.0.0.0', port=8080, debug=True)
