import dlt
import sys
import os
import logging
from datetime import datetime 
from typing import Iterable
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.point import Point
import cloudscraper

# Add project root to sys.path for import resolution
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
    
from src.pipelines.resources.trading_properties_schemas import ImovelRegister, PriceRegister
from src.pipelines.resources.trading_properties_function_classes import zapImoveis
from src.pipelines.resources.common.common_functions import make_propertie_id
from src.pipelines.resources.config_loader import config

# Load configurations
ZAP_CONFIG = config.get_source_config('zap_imoveis')
GEOCODING_CONFIG = config.get_geocoding_config()
DATABASE_CONFIG = config.get_database_config()
LOGGING_CONFIG = config.get_logging_config()
SCRAPER_SETTINGS = config.get_scraper_settings()

# Get max pages from environment variable if set
MAX_PAGES = int(os.environ.get('KODOMIYA_MAX_PAGES', 0)) or None


def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("zap_imoveis_pipeline")
    logger.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    formatter = logging.Formatter(LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger


logger = setup_logging()
logger.info("Initializing Zap Imóveis pipeline")

if MAX_PAGES:
    logger.info(f"Page limit set to {MAX_PAGES} pages")
else:
    logger.info("No page limit set - will scrape all available pages")


@dlt.resource(
    name="zap_imoveis_register", 
    write_disposition="merge", 
    primary_key="id", 
    columns=ImovelRegister
)
def generate_zap_imoveis_register(
    base_url: str = ZAP_CONFIG['base_url'],
    propertie_html_class: str = ZAP_CONFIG['property_card']['html_class'],
    propertie_html_element: str = ZAP_CONFIG['property_card']['html_element'],
    page_number: int = 1,
    search_lat_long_view_box: list[Point, Point] = [
        Point(ZAP_CONFIG['search_lat_long_view_box'][0][0], ZAP_CONFIG['search_lat_long_view_box'][0][1]),
        Point(ZAP_CONFIG['search_lat_long_view_box'][1][0], ZAP_CONFIG['search_lat_long_view_box'][1][1])
    ]
) -> Iterable[dict]:
    """Generate property registration data from Zap Imóveis."""
    logger.info("Starting Zap Imóveis property register scraping")
    logger.info(f"Using base URL: {base_url}")
    
    geolocator = Nominatim(user_agent=GEOCODING_CONFIG['user_agent'])
    logger.debug(f"Initialized geocoder with user agent: {GEOCODING_CONFIG['user_agent']}")

    scraper = cloudscraper.create_scraper()
    properties_count = 0
    previous_page_ids = set()

    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping.")
            break
            
        url = base_url + f"{ZAP_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            print(url)
            response = scraper.get(url, allow_redirects=False)
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
                
                price = zapImoveis.return_zap_imoveis_preco(
                    card_imovel, 
                    ZAP_CONFIG['price']
                )
                logger.debug(f"Extracted price: {price}")

                size = zapImoveis.return_zap_imoveis_tamanho(
                    card_imovel,
                    ZAP_CONFIG['size']
                )
                logger.debug(f"Extracted size: {size}")

                bedrooms = zapImoveis.return_zap_imoveis_n_quartos(
                    card_imovel,
                    ZAP_CONFIG['rooms']
                )
                logger.debug(f"Extracted rooms: {bedrooms}")

                bathrooms = zapImoveis.return_zap_imoveis_n_banheiros(
                    card_imovel,
                    ZAP_CONFIG['bathrooms']
                )
                logger.debug(f"Extracted bathrooms: {bathrooms}")

                parking = zapImoveis.return_zap_imoveis_n_vagas_garagem(
                    card_imovel,
                    ZAP_CONFIG['parking']
                )
                logger.debug(f"Extracted parking spaces: {parking}")

                street, neighborhood, city = zapImoveis.return_zap_imoveis_endereco(
                    card_imovel,
                    ZAP_CONFIG['address']
                )
                logger.debug(f"Extracted address: {street}, {neighborhood}, {city}")

                address = f"{str(street).strip().title()} - {str(city).strip().title()} - PR"
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

                property_id = make_propertie_id(list_of_string_to_concatenate=[street, neighborhood, city])
                logger.debug(f"Generated property ID: {property_id}")

                current_page_ids.add(property_id)
                if property_id in previous_page_ids:
                    duplicates_found += 1

                property_data = {
                    "id": property_id,
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


@dlt.resource(
    name="zap_imoveis_history", 
    write_disposition="append", 
    primary_key="id", 
    columns=PriceRegister
)
def generate_zap_imoveis_history(
    base_url: str = ZAP_CONFIG['base_url'],
    propertie_html_class: str = ZAP_CONFIG['property_card']['html_class'],
    propertie_html_element: str = ZAP_CONFIG['property_card']['html_element'],
    page_number: int = 1
) -> Iterable[dict]:
    """Generate price history data from Zap Imóveis."""
    logger.info("Starting Zap Imóveis price history scraping")
    
    scraper = cloudscraper.create_scraper()
    history_count = 0
    previous_page_ids = set()
    
    while True:
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping price history scraping.")
            break
            
        url = base_url + f"{ZAP_CONFIG['pagination_param']}{page_number}"
        logger.info(f"Scraping price history page {page_number} - URL: {url}")

        try:
            logger.debug(f"Sending HTTP request to {url}")
            response = scraper.get(url, allow_redirects=False)
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
                
                price = zapImoveis.return_zap_imoveis_preco(
                    card_imovel,
                    ZAP_CONFIG['price']
                )
                logger.debug(f"Extracted price: {price}")

                street, neighborhood, city = zapImoveis.return_zap_imoveis_endereco(
                    card_imovel,
                    ZAP_CONFIG['address']
                )
                logger.debug(f"Extracted address: {street}, {neighborhood}, {city}")

                property_id = make_propertie_id(list_of_string_to_concatenate=[street, neighborhood, city])
                logger.debug(f"Generated property ID: {property_id}")

                current_page_ids.add(property_id)
                if property_id in previous_page_ids:
                    duplicates_found += 1

                history_data = {
                    "id": property_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": price,
                }
                
                history_count += 1
                logger.debug(f"Yielding price history data: {history_data}")
                yield history_data

            if (SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and 
                    duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and 
                    len(current_page_ids) > 0):
                logger.warning(f"Stopping price history scraping due to duplicate content. Found {duplicates_found} duplicates. "
                             f"Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
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
def generate_zap_imoveis():
    """Generate Zap Imóveis data source."""
    logger.info("Registering Zap Imóveis resources")
    yield generate_zap_imoveis_register
    yield generate_zap_imoveis_history


logger.info("Creating Zap Imóveis DLT pipeline")
pipeline = dlt.pipeline(
    pipeline_name="kodomiya",
    dataset_name="kodomiya_zap_imoveis",
    destination=dlt.destinations.duckdb(fr"{DATABASE_CONFIG['path']}/kodomiya.duckdb"),
)

logger.info("Running Zap Imóveis pipeline")

try:
    pipeline_result = pipeline.run(generate_zap_imoveis())
    logger.info(f"Pipeline completed successfully: {pipeline_result}")

except Exception as e:
    logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
    pipeline_result = {"error": str(e), "message": "Pipeline execution failed before completion."}

logger.info("Zap Imóveis pipeline execution finished")
