import os
import sys
import logging
import time
import requests

# Add project root to sys.path for import resolution
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.pipelines.pipeline_deduplication import deduplicate_properties
from src.pipelines.resources.config_loader import config

# Telegram Configuration
TELEGRAM_CONFIG = config.get_telegram_config()
TELEGRAM_BOT_TOKEN = TELEGRAM_CONFIG.get('bot_token')
TELEGRAM_CHAT_ID = TELEGRAM_CONFIG.get('chat_id')

def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("run_deduplication_pipeline")
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def escape_markdown(text):
    """Escape special characters for Telegram Markdown."""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!', '\\']
    
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    
    return text

def send_telegram_message(message):
    """Sends a message to a Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram bot token or chat ID is not configured. Skipping message.")
        return
        
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(url, data=payload, timeout=20)
        response.raise_for_status()
        logger.info("Telegram notification sent.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")

if __name__ == "__main__":
    logger = setup_logging()
    
    pipeline_display_name = "Property Deduplication"
    
    logger.info(f"Starting pipeline: {pipeline_display_name}")
    send_telegram_message(f"🚀 Starting pipeline: *{pipeline_display_name}*")
    
    start_time = time.time()
    success = False

    try:
        deduplicate_properties()
        success = True
        logger.info(f"Pipeline '{pipeline_display_name}' executed successfully.")

    except Exception as e:
        logger.error(f"Error running pipeline '{pipeline_display_name}': {e}", exc_info=True)
        error_message = escape_markdown(str(e))
        send_telegram_message(f"❌ Error executing pipeline *{pipeline_display_name}*: {error_message}")
        success = False

    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Execution of {pipeline_display_name} took {duration:.2f} seconds.")

        if success:
            success_message = (
                f"✅ Pipeline *{pipeline_display_name}* finished successfully.\n"
                f"⏱️ Duration: {duration:.2f} seconds."
            )
            send_telegram_message(success_message)
        else:
            send_telegram_message(f"Pipeline *{pipeline_display_name}* finished with issues. Duration: {duration:.2f}s")
    
    logger.info("Deduplication run finished.") 