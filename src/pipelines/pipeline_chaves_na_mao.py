# DLT imports
import dlt
from dlt.sources.helpers import requests

# External imports
from geopy.geocoders import Nominatim
from geopy.point import Point
from datetime import datetime
from typing import Iterable
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import logging
import sys
import os

# Adjust Python path to recognize 'src' module when script is run directly
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Local imports
from src.pipelines.resources.trading_properties_schemas import ImovelRegister, PriceRegister
from src.pipelines.resources.trading_properties_function_classes import chavesNaMao
from src.pipelines.resources.common.common_functions import make_propertie_id
from src.pipelines.resources.config_loader import config

# Load configurations
CHAVES_CONFIG = config.get_source_config('chaves_na_mao')
GEOCODING_CONFIG = config.get_geocoding_config()
DATABASE_CONFIG = config.get_database_config()
LOGGING_CONFIG = config.get_logging_config()
SCRAPER_SETTINGS = config.get_scraper_settings()

# Get max pages from environment variable if set
MAX_PAGES = int(os.environ.get('KODOMIYA_MAX_PAGES', 0)) or None

def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("chaves_na_mao_pipeline")
    logger.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    formatter = logging.Formatter(LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

# Initialize logger
logger = setup_logging()
logger.info("Initializing Chaves na Mão pipeline")
if MAX_PAGES:
    logger.info(f"Page limit set to {MAX_PAGES} pages")
else:
    logger.info("No page limit set - will scrape all available pages")

@dlt.resource(name="chaves_na_mao_register", write_disposition="merge", primary_key="id", columns=ImovelRegister)
def generate_chaves_na_mao_register(
    base_url: str = CHAVES_CONFIG['base_url'],
    propertie_html_class: str = CHAVES_CONFIG['property_card']['html_class'],
    propertie_html_element: str = CHAVES_CONFIG['property_card']['html_element'],
    page_number: int = 1,
    search_lat_long_view_box: list[Point, Point] = [
        Point(CHAVES_CONFIG['search_lat_long_view_box'][0][0], CHAVES_CONFIG['search_lat_long_view_box'][0][1]),
        Point(CHAVES_CONFIG['search_lat_long_view_box'][1][0], CHAVES_CONFIG['search_lat_long_view_box'][1][1])
    ]
) -> Iterable[dict]:
    """Generate property registration data by scraping listings."""
    logger.info("Starting Chaves na Mão property register scraping")
    logger.info(f"Using base URL: {base_url}")
    
    geolocator = Nominatim(user_agent=GEOCODING_CONFIG['user_agent'])
    logger.debug(f"Initialized geocoder with user agent: {GEOCODING_CONFIG['user_agent']}")
    
    properties_count = 0
    previous_page_ids = set()
    
    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping.")
            break
            
        url = base_url + f"{CHAVES_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            print(url)
            response = requests.get(url, allow_redirects=False)
            logger.debug(f"Received response with status code: {response.status_code}")

        except HTTPError as e:
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            break

        if response.status_code == 200:
            logger.debug("Parsing HTML content with BeautifulSoup")
            soup = BeautifulSoup(response.content, "html.parser")

            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)
            logger.info(f"Found {len(cards_imoveis)} property cards on page {page_number}")

            current_page_ids = set()
            duplicates_found = 0

            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing property card {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                # Extract property details
                price = chavesNaMao.return_chaves_na_mao_preco(
                    card_imovel, 
                    CHAVES_CONFIG['price']['tag'], 
                    CHAVES_CONFIG['price']['class_name'],
                    CHAVES_CONFIG['price']['price_value_tag'],
                )
                
                size = chavesNaMao.return_chaves_na_mao_tamanho(
                    card_imovel,
                    CHAVES_CONFIG['size']['tag'],
                    CHAVES_CONFIG['size']['class_name'],
                    index=CHAVES_CONFIG['size'].get('index', 0),
                    split_text=CHAVES_CONFIG['size'].get('split_text')
                )
                
                bedrooms = chavesNaMao.return_chaves_na_mao_n_quartos(
                    card_imovel,
                    CHAVES_CONFIG['rooms']['tag'],
                    CHAVES_CONFIG['rooms']['class_name'],
                    CHAVES_CONFIG['rooms']['search_text']
                )
                
                bathrooms = chavesNaMao.return_chaves_na_mao_n_banheiros(
                    card_imovel,
                    CHAVES_CONFIG['bathrooms']['tag'],
                    CHAVES_CONFIG['bathrooms']['class_name'],
                    CHAVES_CONFIG['bathrooms']['search_text']
                )
                
                parking = chavesNaMao.return_chaves_na_mao_n_vagas_garagem(
                    card_imovel,
                    CHAVES_CONFIG['parking']['tag'],
                    CHAVES_CONFIG['parking']['class_name'],
                    CHAVES_CONFIG['parking']['search_text']
                )

                street, neighborhood, city = chavesNaMao.return_chaves_na_mao_endereco(
                    card_imovel,
                    CHAVES_CONFIG['address']['main_tag'],
                    CHAVES_CONFIG['address']['class_name'],
                    CHAVES_CONFIG['address']['rua_tag'],
                    CHAVES_CONFIG['address']['rua_index'],
                    CHAVES_CONFIG['address']['bairro_cidade_tag'],
                    CHAVES_CONFIG['address']['bairro_cidade_index']
                )

                # Geocode the address
                address = f"{street.strip().title()} - {city.strip().title()} - PR"
                logger.debug(f"Geocoding address: {address}")
                
                try:
                    geolocator_info = geolocator.geocode(
                        address, 
                        viewbox=search_lat_long_view_box, 
                        country_codes=GEOCODING_CONFIG['country_codes'], 
                        timeout=GEOCODING_CONFIG['timeout'], 
                        bounded=GEOCODING_CONFIG['bounded']
                    )
                    latitude = getattr(geolocator_info, "latitude", None)
                    longitude = getattr(geolocator_info, "longitude", None)
                    logger.debug(f"Geocoding result: lat={latitude}, long={longitude}")
                
                except Exception as e:
                    logger.error(f"Error geocoding address '{address}': {str(e)}")
                    latitude = None
                    longitude = None

                propertie_id = make_propertie_id([street, neighborhood, city])
                logger.debug(f"Generated property ID: {propertie_id}")

                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1

                property_data = {
                    "id": propertie_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": price,
                    "tamanho": size,
                    "n_quartos": bedrooms,
                    "n_banheiros": bathrooms,
                    "n_garagem": parking,
                    "rua": street,
                    "bairro": neighborhood,
                    "cidade": city,
                    "latitude": latitude,
                    "longitude": longitude,
                }
                
                properties_count += 1
                logger.debug(f"Yielding property data: {property_data}")
                yield property_data

            # Check for duplicate page content
            if (SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and 
                duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and 
                len(current_page_ids) > 0):
                logger.warning(f"Stopping due to duplicate content. Found {duplicates_found} duplicates from previous page. "
                             f"Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids
            page_number += 1
            logger.info(f"Moving to next page: {page_number}")

        else:
            logger.warning(f"Got non-200 status code ({response.status_code}) for page {page_number}")
            logger.info(f"Stopping pagination at page {page_number-1}")
            break 
            
    logger.info(f"Completed register scraping. Total properties processed: {properties_count}")

@dlt.resource(name="chaves_na_mao_history", write_disposition="append", primary_key="id", columns=PriceRegister)
def generate_chaves_na_mao_history(
    base_url: str = CHAVES_CONFIG['base_url'],
    propertie_html_class: str = CHAVES_CONFIG['property_card']['html_class'],
    propertie_html_element: str = CHAVES_CONFIG['property_card']['html_element'],
    page_number: int = 1
) -> Iterable[dict]:
    """Generate historical price data for properties."""
    logger.info("Starting Chaves na Mão price history scraping")
    history_count = 0
    previous_page_ids = set()
    
    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping price history scraping.")
            break
            
        url = base_url + f"{CHAVES_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping price history page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            response = requests.get(url, allow_redirects=False)
            logger.debug(f"Received response with status code: {response.status_code}")

        except HTTPError as e:
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            break 

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)
            logger.info(f"Found {len(cards_imoveis)} property cards for price history on page {page_number}")

            current_page_ids = set()
            duplicates_found = 0

            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing price history for property {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                price = chavesNaMao.return_chaves_na_mao_preco(
                    card_imovel,
                    CHAVES_CONFIG['price']['tag'],
                    CHAVES_CONFIG['price']['class_name'],
                    CHAVES_CONFIG['price']['price_value_tag'],
                )

                street, neighborhood, city = chavesNaMao.return_chaves_na_mao_endereco(
                    card_imovel,
                    CHAVES_CONFIG['address']['main_tag'],
                    CHAVES_CONFIG['address']['class_name'],
                    CHAVES_CONFIG['address']['rua_tag'],
                    CHAVES_CONFIG['address']['rua_index'],
                    CHAVES_CONFIG['address']['bairro_cidade_tag'],
                    CHAVES_CONFIG['address']['bairro_cidade_index']
                )

                propertie_id = make_propertie_id([street, neighborhood, city])
                
                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1

                history_data = {
                    "id": propertie_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": price,
                }
                
                history_count += 1
                logger.debug(f"Yielding price history data: {history_data}")
                yield history_data

            if (SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and 
                duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and 
                len(current_page_ids) > 0):
                logger.warning(f"Stopping price history scraping due to duplicate content. Found {duplicates_found} duplicates "
                             f"from previous page. Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids
            page_number += 1
            logger.info(f"Moving to next price history page: {page_number}")

        else:
            logger.warning(f"Got non-200 status code ({response.status_code}) for price history page {page_number}")
            logger.info(f"Stopping price history pagination at page {page_number-1}")
            break
            
    logger.info(f"Completed price history scraping. Total history records processed: {history_count}")

@dlt.source
def generate_chaves_na_mao():
    """Combine all Chaves na Mão data resources."""
    logger.info("Registering Chaves na Mão resources")
    yield generate_chaves_na_mao_register
    yield generate_chaves_na_mao_history

# Create and run DLT pipeline
logger.info("Creating Chaves na Mão DLT pipeline")
pipeline = dlt.pipeline(
    pipeline_name="kodomiya",
    dataset_name="kodomiya_chaves_na_mao",
    destination=dlt.destinations.duckdb(fr"{DATABASE_CONFIG['path']}/kodomiya.duckdb"),
)

logger.info("Running Chaves na Mão pipeline")

try:
    pipeline_result = pipeline.run(generate_chaves_na_mao())
    logger.info(f"Pipeline completed successfully: {pipeline_result}")

except Exception as e:
    logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
    pipeline_result = {"error": str(e), "message": "Pipeline execution failed before completion."}

logger.info("Chaves na Mão pipeline execution finished")
