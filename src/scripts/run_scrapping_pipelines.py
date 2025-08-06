import importlib.util
import os
import sys
import logging
import time
import requests


_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.pipelines.resources.config_loader import config


TELEGRAM_CONFIG = config.get_telegram_config()
TELEGRAM_BOT_TOKEN = TELEGRAM_CONFIG.get('bot_token')
TELEGRAM_CHAT_ID = TELEGRAM_CONFIG.get('chat_id')


def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("run_scrapping_pipelines")
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger


logger = setup_logging()


def escape_markdown(text):
    """Escape special characters for Telegram Markdown."""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!', '\\']
    
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    
    return text


def send_telegram_message(message):
    """Sends a message to a Telegram chat using the requests library."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }

    response = requests.post(url, data=payload, timeout=20)
    response.raise_for_status()
    logger.info(f"Telegram notification sent (first 100 chars): {message[:100]}...")


def run_pipeline_module(pipeline_module_name: str, pipeline_display_name: str):
    """
    Runs a single DLT pipeline by importing its module sequentially.
    The pipeline script configures and runs its DLT pipeline,
    storing the LoadInfo object in a global variable 'pipeline_result'.
    Sends Telegram notifications for start, completion/failure, duration, and DLT load info.
    """
    send_telegram_message(f"🚀 Starting pipeline: *{pipeline_display_name}*")
    logger.info(f"Running pipeline: {pipeline_display_name} (Module: {pipeline_module_name})")
    
    start_time = time.time()
    dlt_load_info_str = "Load info not available or pipeline did not set 'pipeline_result'."
    success = False

    try:
        pipeline_path = os.path.join(_PROJECT_ROOT, 'src', 'pipelines', f"{pipeline_module_name}.py")
        logger.info(f"Attempting to import and execute pipeline module: {pipeline_module_name} from {pipeline_path}")

        module_import_name = f"kodomiya.pipelines.sequential.{pipeline_module_name}_{time.time_ns()}"
        spec = importlib.util.spec_from_file_location(module_import_name, pipeline_path)
        
        if spec is None:
            logger.error(f"Could not load spec for module {pipeline_module_name} from {pipeline_path}")
            send_telegram_message(f"❌ Failed to load pipeline spec: *{pipeline_display_name}*.")
            return
        
        pipeline_module = importlib.util.module_from_spec(spec)
        
        if spec.loader is None:
            logger.error(f"No loader found for module spec {pipeline_module_name}")
            send_telegram_message(f"❌ Failed to load pipeline: *{pipeline_display_name}* (no loader).")
            return

        sys.modules[module_import_name] = pipeline_module 
        spec.loader.exec_module(pipeline_module)
        
        load_info = None
        if hasattr(pipeline_module, "pipeline_result"):
            load_info = getattr(pipeline_module, "pipeline_result")
            dlt_load_info_str = str(load_info)

        if load_info:
            if load_info.has_failed_jobs:
                logger.error(f"Pipeline {pipeline_display_name} reported failed DLT jobs. LoadInfo: {dlt_load_info_str}")
                success = False
            
            else:
                logger.info(f"Pipeline {pipeline_display_name} (Module: {pipeline_module_name}) completed DLT execution.")
                success = True
        
        else:
            success = True

        if success:
            logger.info(f"Pipeline module {pipeline_display_name} executed.")

    except Exception as e:
        logger.error(f"Error running pipeline module {pipeline_display_name} (Module: {pipeline_module_name}): {e}", exc_info=True)
        send_telegram_message(f"❌ Error executing pipeline *{pipeline_display_name}*: {e}")
        success = False

    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Execution of {pipeline_display_name} took {duration:.2f} seconds.")

        if module_import_name in sys.modules:
            del sys.modules[module_import_name]
        
        if success:
            # Filter out the sensitive/verbose path line from DLT info
            lines = dlt_load_info_str.splitlines()
            filtered_lines = [line for line in lines if not line.strip().startswith("The duckdb destination used")]
            filtered_dlt_info = "\n".join(filtered_lines)

            # Construct the success message
            # pipeline_display_name is used within *...* for bold. Assumed to be safe.
            # filtered_dlt_info is placed directly in a code block, so no escaping is needed for it.
            success_message = (
                f"✅ Pipeline *{pipeline_display_name}* finished successfully.\n"
                f"⏱️ Duration: {duration:.2f} seconds.\n"
                f"📊 DLT Load Info:\n```{filtered_dlt_info}```"
            )
            send_telegram_message(success_message)
        else:
            send_telegram_message(f"Pipeline *{pipeline_display_name}* finished with issues. Duration: {duration:.2f}s")


if __name__ == "__main__":
    logger.info("Starting sequential pipeline execution...")
    overall_start_time = time.time()

    pipelines_dir = os.path.join(_PROJECT_ROOT, 'src', 'pipelines')
    logger.info(f"Looking for pipelines in: {pipelines_dir}")

    pipeline_files = [
        "pipeline_chaves_na_mao.py",
        "pipeline_viva_real.py",
        "pipeline_zap_imoveis.py",
        "pipeline_leilao_imovel.py",
        "pipeline_deduplication.py",
    ]

    logger.info(f"Found pipeline files: {pipeline_files}")
    
    if not pipeline_files:
        logger.warning("No pipeline files found in src/pipelines starting with 'pipeline_'.")
        send_telegram_message("⚠️ No scraping pipelines found to run.")
    
    else:
        total_pipelines = len(pipeline_files)
        
        for i, pipeline_file in enumerate(pipeline_files):
            logger.info(f"--- Running pipeline {i+1}/{total_pipelines} --- ")
            module_name_full = os.path.splitext(pipeline_file)[0]
            source_name_part = module_name_full[len("pipeline_"):]
            pipeline_display_name = source_name_part.replace("_", " ").title()
            
            run_pipeline_module(module_name_full, pipeline_display_name)
            logger.info(f"--- Finished pipeline {i+1}/{total_pipelines}: {pipeline_display_name} ---")

    overall_end_time = time.time()
    overall_duration = overall_end_time - overall_start_time
    logger.info(f"All pipeline executions completed in {overall_duration:.2f} seconds.")
    send_telegram_message(f"🏁 All scraping pipelines have finished their sequential runs. Total time: {overall_duration:.2f}s.")
