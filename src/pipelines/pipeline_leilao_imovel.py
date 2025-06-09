# Imports DLT
import dlt
from dlt.sources.helpers import requests # Using requests, can be switched to cloudscraper if needed

# Imports online
from geopy.geocoders import Nominatim
from geopy.point import Point
from datetime import datetime
from typing import Iterable
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import logging
import sys
import os
import cloudscraper # Added import

# Adjust Python path to recognize 'src' module when script is run directly
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Imports offline
from src.pipelines.resources.trading_properties_schemas import LeilaoImovelRegister, LeilaoImovelHistory
from src.pipelines.resources.trading_properties_function_classes import leilaoImovel # New class
from src.pipelines.resources.common.common_functions import make_propertie_id
from src.pipelines.resources.config_loader import config

# Load configuration for leilao_imovel
LEILAO_CONFIG = config.get_source_config('leilao_imovel')
GEOCODING_CONFIG = config.get_geocoding_config()
DATABASE_CONFIG = config.get_database_config()
LOGGING_CONFIG = config.get_logging_config()
SCRAPER_SETTINGS = config.get_scraper_settings()

# Get max pages from environment variable if set
MAX_PAGES = int(os.environ.get('KODOMIYA_MAX_PAGES', 0)) or None

# Setup logging
def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("leilao_imovel_pipeline")
    logger.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    formatter = logging.Formatter(LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

# Initialize logger
logger = setup_logging()
logger.info("Initializing Leilão Imóvel pipeline")
if MAX_PAGES:
    logger.info(f"Page limit set to {MAX_PAGES} pages")
else:
    logger.info("No page limit set - will scrape all available pages")

# Resource for property registration
@dlt.resource(name="leilao_imovel_register", write_disposition="merge", primary_key="id", columns=LeilaoImovelRegister)
def generate_leilao_imovel_register(
    base_url: str = LEILAO_CONFIG['base_url'],
    propertie_html_class: str = LEILAO_CONFIG['property_card']['html_class'],
    propertie_html_element: str = LEILAO_CONFIG['property_card']['html_element'],
    page_number: int = 1,
    search_lat_long_view_box: list[Point, Point] = [
        Point(LEILAO_CONFIG['search_lat_long_view_box'][0][0], LEILAO_CONFIG['search_lat_long_view_box'][0][1]),
        Point(LEILAO_CONFIG['search_lat_long_view_box'][1][0], LEILAO_CONFIG['search_lat_long_view_box'][1][1])
    ]
) -> Iterable[dict]:
    """Scrape and generate property registration data."""
    logger.info("Starting Leilão Imóvel property register scraping")
    logger.info(f"Using base URL: {base_url}")
    geolocator = Nominatim(user_agent=GEOCODING_CONFIG['user_agent'])
    logger.debug(f"Initialized geocoder with user agent: {GEOCODING_CONFIG['user_agent']}")
    
    # Using the requests library as the scraper instance for this pipeline
    scraper_instance = cloudscraper.create_scraper() # Changed to cloudscraper

    properties_count = 0
    previous_page_ids = set()
    
    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping.")
            break
            
        url = base_url + f"{LEILAO_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            response = scraper_instance.get(url, allow_redirects=True) # Allow redirects for initial page
            logger.debug(f"Received response with status code: {response.status_code}")
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Exception while accessing page {page_number}: {str(e)}")
            break

        if response.status_code == 200:
            logger.debug("Parsing HTML content with BeautifulSoup")
            soup = BeautifulSoup(response.content, "html.parser")
            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)

            # Remove last property due to known issue with random 2023 property
            cards_imoveis = cards_imoveis[:-1]
            logger.info(f"Found {len(cards_imoveis)} property cards on page {page_number}")

            if not cards_imoveis and page_number > 1:
                logger.info(f"No property cards found on page {page_number}. Assuming end of results.")
                break

            current_page_ids = set()
            duplicates_found = 0

            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing property card {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                # Extract link_detalhes
                link_detalhes = leilaoImovel.return_leilao_imovel_link_detalhes(
                    card_imovel,
                    LEILAO_CONFIG # Pass the whole config dict
                )
                logger.debug(f"Extracted details link: {link_detalhes}")

                preco_primeira_praca, data_primeira_praca, preco_segunda_praca, data_segunda_praca, preco_atual = leilaoImovel.return_leilao_imovel_praca_info(
                    card_imovel,
                    LEILAO_CONFIG # Pass the whole leilao_config
                )
                logger.debug(f"Praça Info: 1st Price: {preco_primeira_praca}, 1st Date: {data_primeira_praca}, "
                           f"2nd Price: {preco_segunda_praca}, 2nd Date: {data_segunda_praca}, Current Price: {preco_atual}")

                rua, bairro, cidade = leilaoImovel.return_leilao_imovel_endereco(
                    card_imovel,
                    LEILAO_CONFIG['address']
                )
                logger.debug(f"Extracted address: {rua}, {bairro}, {cidade}")
                
                # Initialize area fields as None, to be fetched later if link_detalhes is available
                area_util = None
                area_terreno = None
                aceita_financiamento = None
                aceita_fgts = None
                n_garagem = None
                n_quartos = None

                if link_detalhes:
                    # Fetch and parse the details page for tamanho and other info
                    details_page_data = leilaoImovel.return_leilao_imovel_details_page_info(
                        link_detalhes, 
                        LEILAO_CONFIG['details_page_selectors'], # Updated to pass the new parent config key
                        scraper_instance
                    )
                    area_util = details_page_data.get("area_util")
                    area_terreno = details_page_data.get("area_terreno")
                    aceita_financiamento = details_page_data.get("aceita_financiamento")
                    aceita_fgts = details_page_data.get("aceita_fgts")
                    n_garagem = details_page_data.get("n_garagem")
                    n_quartos = details_page_data.get("n_quartos")

                latitude, longitude = None, None
                if rua and cidade: 
                    endereco_completo = f"{str(rua).strip().title()}, {str(bairro).strip().title()} - {str(cidade).strip().title()} - PR"
                    logger.debug(f"Geocoding address: {endereco_completo}")
                    try:
                        geolocator_info = geolocator.geocode(
                            endereco_completo, 
                            viewbox=search_lat_long_view_box, 
                            country_codes=GEOCODING_CONFIG['country_codes'], 
                            timeout=GEOCODING_CONFIG['timeout'], 
                            bounded=GEOCODING_CONFIG['bounded']
                        )
                        if geolocator_info:
                            latitude = geolocator_info.latitude
                            longitude = geolocator_info.longitude
                        logger.debug(f"Geocoding result: lat={latitude}, long={longitude}")
                    except Exception as e:
                        logger.error(f"Error geocoding address '{endereco_completo}': {str(e)}")
                
                # Use a combination of key elements for ID to handle cases where address might be less unique initially
                id_elements = [rua, bairro, cidade, str(preco_primeira_praca), str(data_primeira_praca),
                             str(preco_segunda_praca), str(data_segunda_praca)]
                propertie_id_string = "".join(filter(None, id_elements)) # Filter out None before joining
                
                if not propertie_id_string: # If all main components are None/empty, this is bad data
                    logger.warning(f"Could not generate a valid ID for property card {i+1} on page {page_number}. Skipping.")
                    continue

                propertie_id = make_propertie_id(list_of_string_to_concatenate=[propertie_id_string])
                logger.debug(f"Generated property ID: {propertie_id}")

                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1

                property_data = {
                    "id": propertie_id,
                    "datahora": datetime.now(),
                    "preco_primeira_praca": preco_primeira_praca,
                    "data_primeira_praca": data_primeira_praca,
                    "preco_segunda_praca": preco_segunda_praca,
                    "data_segunda_praca": data_segunda_praca,
                    "preco_atual": preco_atual,
                    "area_util": area_util,
                    "area_terreno": area_terreno,
                    "rua": rua,
                    "bairro": bairro,
                    "cidade": cidade,
                    "latitude": latitude,
                    "longitude": longitude,
                    "link_detalhes": link_detalhes,
                    "aceita_financiamento": aceita_financiamento,
                    "aceita_fgts": aceita_fgts,
                    "n_garagem": n_garagem,
                    "n_quartos": n_quartos
                }
                
                properties_count += 1
                logger.debug(f"Yielding property data: {property_data}")
                yield property_data

            if SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and len(current_page_ids) > 0:
                logger.warning(f"Stopping due to duplicate content. Found {duplicates_found} duplicates from previous page. Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids
            page_number += 1
            logger.info(f"Moving to next page: {page_number}")
        else:
            logger.warning(f"Got non-200 status code ({response.status_code}) for page {page_number}")
            logger.info(f"Stopping pagination at page {page_number-1}")
            break 
            
    logger.info(f"Completed register scraping. Total properties processed: {properties_count}")

# Resource for property price history
@dlt.resource(name="leilao_imovel_history", write_disposition="append", primary_key="id", columns=LeilaoImovelHistory)
def generate_leilao_imovel_history(
    base_url: str = LEILAO_CONFIG['base_url'],
    propertie_html_class: str = LEILAO_CONFIG['property_card']['html_class'],
    propertie_html_element: str = LEILAO_CONFIG['property_card']['html_element'],
    page_number: int = 1
) -> Iterable[dict]:
    """Scrape and generate property price history data."""
    logger.info("Starting Leilão Imóvel price history scraping")
    scraper_instance = cloudscraper.create_scraper() # Changed to cloudscraper
    history_count = 0
    previous_page_ids = set()
    
    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping price history scraping.")
            break
            
        url = base_url + f"{LEILAO_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping price history page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            response = scraper_instance.get(url, allow_redirects=True)
            logger.debug(f"Received response with status code: {response.status_code}")
            response.raise_for_status()
        except HTTPError as e:
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Exception while accessing page {page_number}: {str(e)}")
            break

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)
            logger.info(f"Found {len(cards_imoveis)} property cards for price history on page {page_number}")

            if not cards_imoveis and page_number > 1:
                logger.info(f"No property cards for history found on page {page_number}. Assuming end of results.")
                break

            current_page_ids = set()
            duplicates_found = 0

            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing price history for property {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                preco_primeira_praca, data_primeira_praca, preco_segunda_praca, data_segunda_praca, preco_atual = leilaoImovel.return_leilao_imovel_praca_info(
                    card_imovel,
                    LEILAO_CONFIG
                )
                logger.debug(f"Extracted Praça Info for history: 1st P: {preco_primeira_praca}, 1st D: {data_primeira_praca}, "
                           f"2nd P: {preco_segunda_praca}, 2nd D: {data_segunda_praca}, Current P: {preco_atual}")

                rua, bairro, cidade = leilaoImovel.return_leilao_imovel_endereco(
                    card_imovel,
                    LEILAO_CONFIG['address']
                )
                # ID generation should be consistent with the register resource
                id_elements = [rua, bairro, cidade, str(preco_primeira_praca), str(data_primeira_praca),
                             str(preco_segunda_praca), str(data_segunda_praca)]
                propertie_id_string = "".join(filter(None, id_elements))

                if not propertie_id_string:
                    logger.warning(f"Could not generate a valid ID for history property card {i+1} on page {page_number}. Skipping.")
                    continue
                
                propertie_id = make_propertie_id(list_of_string_to_concatenate=[propertie_id_string])
                logger.debug(f"Generated property ID for history: {propertie_id}")

                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1
                
                history_data = {
                    "id": propertie_id,
                    "datahora": datetime.now(),
                    "preco_primeira_praca": preco_primeira_praca,
                    "data_primeira_praca": data_primeira_praca,
                    "preco_segunda_praca": preco_segunda_praca,
                    "data_segunda_praca": data_segunda_praca,
                    "preco_atual": preco_atual,
                }
                
                history_count += 1
                logger.debug(f"Yielding price history data: {history_data}")
                yield history_data

            if SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and len(current_page_ids) > 0:
                logger.warning(f"Stopping price history scraping due to duplicate content. Found {duplicates_found} duplicates. Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids
            page_number += 1
            logger.info(f"Moving to next price history page: {page_number}")
        else:
            logger.warning(f"Got non-200 status code ({response.status_code}) for price history page {page_number}")
            logger.info(f"Stopping price history pagination at page {page_number-1}")
            break
            
    logger.info(f"Completed price history scraping. Total history records processed: {history_count}")

# DLT source definition
@dlt.source
def generate_leilao_imovel_source():
    """Register Leilão Imóvel resources."""
    logger.info("Registering Leilão Imóvel resources")
    yield generate_leilao_imovel_register
    yield generate_leilao_imovel_history

# DLT pipeline definition
logger.info("Creating Leilão Imóvel DLT pipeline")
pipeline = dlt.pipeline(
    pipeline_name="kodomiya",
    dataset_name="kodomiya_leilao_imovel",
    destination=dlt.destinations.duckdb(f"{DATABASE_CONFIG['path']}/kodomiya.duckdb"),
)

logger.info("Running Leilão Imóvel pipeline")

try:
    pipeline_result = pipeline.run(generate_leilao_imovel_source())
    logger.info(f"Pipeline completed successfully: {pipeline_result}")

except Exception as e:
    logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
    pipeline_result = {"error": str(e), "message": "Pipeline execution failed before completion."}

logger.info("Leilão Imóvel pipeline execution finished") 