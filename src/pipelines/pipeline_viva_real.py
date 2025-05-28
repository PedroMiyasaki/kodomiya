# Imports DLT
import dlt
from dlt.sources.helpers import requests

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
import cloudscraper

# Adjust Python path to recognize 'src' module when script is run directly
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Imports offline
from src.pipelines.resources.trading_properties_schemas import ImovelRegister, PriceRegister
from src.pipelines.resources.trading_properties_function_classes import vivaReal
from src.pipelines.resources.common.common_functions import make_propertie_id
from src.pipelines.resources.config_loader import config

# Load configuration for viva_real
VIVA_REAL_CONFIG = config.get_source_config('viva_real')
GEOCODING_CONFIG = config.get_geocoding_config()
DATABASE_CONFIG = config.get_database_config()
LOGGING_CONFIG = config.get_logging_config()
SCRAPER_SETTINGS = config.get_scraper_settings()

# Get max pages from environment variable if set
MAX_PAGES = int(os.environ.get('KODOMIYA_MAX_PAGES', 0)) or None

# Setup logging
def setup_logging():
    """Set up logging configuration."""
    logger = logging.getLogger("viva_real_pipeline")
    logger.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, LOGGING_CONFIG.get('level', 'INFO')))
    
    # Create formatter
    formatter = logging.Formatter(LOGGING_CONFIG.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

# Initialize logger
logger = setup_logging()
logger.info("Initializing Viva Real pipeline")
if MAX_PAGES:
    logger.info(f"Page limit set to {MAX_PAGES} pages")
else:
    logger.info("No page limit set - will scrape all available pages")

# Fazer função para geração do cadastro dos imóveis
@dlt.resource(name="viva_real_register", write_disposition="merge", primary_key="id", columns=ImovelRegister)
def generate_viva_real_register(
    base_url: str = VIVA_REAL_CONFIG['base_url'],
    propertie_html_class: str = VIVA_REAL_CONFIG['property_card']['html_class'],
    propertie_html_element: str = VIVA_REAL_CONFIG['property_card']['html_element'],
    page_number: int = 1,
    search_lat_long_view_box: list[Point, Point] = [
        Point(VIVA_REAL_CONFIG['search_lat_long_view_box'][0][0], VIVA_REAL_CONFIG['search_lat_long_view_box'][0][1]),
        Point(VIVA_REAL_CONFIG['search_lat_long_view_box'][1][0], VIVA_REAL_CONFIG['search_lat_long_view_box'][1][1])
    ]
) -> Iterable[dict]:
    # Criar geo-localizador pré laço de repetição
    logger.info("Starting Viva Real property register scraping")
    logger.info(f"Using base URL: {base_url}")
    geolocator = Nominatim(user_agent=GEOCODING_CONFIG['user_agent'])
    logger.debug(f"Initialized geocoder with user agent: {GEOCODING_CONFIG['user_agent']}")

    scraper = cloudscraper.create_scraper()
    properties_count = 0
    previous_page_ids = set()
    # Inicia o laço de repetição
    while True:
        # Check if we've reached the maximum number of pages
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping.")
            break
            
        # Definir url pagina atual
        url = base_url + f"{VIVA_REAL_CONFIG['pagination_param']}{page_number}"

        # Mostra página atual iterada
        logger.info(f"Scraping page {page_number} - URL: {url}")

        # Tentar pegar a response
        try:
            logger.debug(f"Sending HTTP request to {url}")
            print(url)
            response = scraper.get(url, allow_redirects=False)
            logger.debug(f"Received response with status code: {response.status_code}")

        # Em caso de erro, pare a função
        except HTTPError as e:
            # Mostre a url atual
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            
            # Pare a função
            break 

        # Se o status vier 200, prossiga
        if response.status_code == 200:
            # Pegar sopa de letras com o BeautifulSoup
            logger.debug("Parsing HTML content with BeautifulSoup")
            soup = BeautifulSoup(response.content, "html.parser")

            # Pegar todos os cards de imoveis anunciados
            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)
            logger.info(f"Found {len(cards_imoveis)} property cards on page {page_number}")

            current_page_ids = set()
            duplicates_found = 0

            # Iterar todos os cards de imóvel
            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing property card {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                # Pegar campo de preço do imovel
                preco = vivaReal.return_viva_real_preco(
                    card_imovel,
                    VIVA_REAL_CONFIG['price']
                )
                logger.debug(f"Extracted price: {preco}")

                # Pegar campo de tamanho do imóvel
                tamanho = vivaReal.return_viva_real_tamanho(
                    card_imovel,
                    VIVA_REAL_CONFIG['size']
                )
                logger.debug(f"Extracted size: {tamanho}")

                # Pegar campo do numero de quartos do imovel
                n_quartos = vivaReal.return_viva_real_n_quartos(
                    card_imovel,
                    VIVA_REAL_CONFIG['rooms']
                )
                logger.debug(f"Extracted rooms: {n_quartos}")

                # Pegar campo do numero de banheiros do imovel
                n_banheiros = vivaReal.return_viva_real_n_banheiros(
                    card_imovel,
                    VIVA_REAL_CONFIG['bathrooms']
                )
                logger.debug(f"Extracted bathrooms: {n_banheiros}")

                # Pegar campo do numero de garagens do imovel
                n_garagem = vivaReal.return_viva_real_n_vagas_garagem(
                    card_imovel,
                    VIVA_REAL_CONFIG['parking']
                )
                logger.debug(f"Extracted parking spaces: {n_garagem}")

                # Pegar o campo de rua, bairro, e cidade
                rua, bairro, cidade = vivaReal.return_viva_real_endereco(
                    card_imovel,
                    VIVA_REAL_CONFIG['address']
                )
                logger.debug(f"Extracted address: {rua}, {bairro}, {cidade}")

                # Captar a lag & long do imóvel
                endereco = str(rua).strip().title() + " - " + str(cidade).strip().title() + " - PR"
                logger.debug(f"Geocoding address: {endereco}")
                
                try:
                    geolocator_info = geolocator.geocode(
                        endereco, 
                        viewbox=search_lat_long_view_box, 
                        country_codes=GEOCODING_CONFIG['country_codes'], 
                        timeout=GEOCODING_CONFIG['timeout'], 
                        bounded=GEOCODING_CONFIG['bounded']
                    )
                    latitude = getattr(geolocator_info, "latitude", None)
                    longitude = getattr(geolocator_info, "longitude", None)
                    logger.debug(f"Geocoding result: lat={latitude}, long={longitude}")
                except Exception as e:
                    logger.error(f"Error geocoding address '{endereco}': {str(e)}")
                    latitude = None
                    longitude = None

                # Gerar id com hash md5 (usar uma junção de rua bairro e cidade)
                propertie_id = make_propertie_id(list_of_string_to_concatenate=[rua, bairro, cidade])
                logger.debug(f"Generated property ID: {propertie_id}")

                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1

                # Retornar o dicionários com os dados do imóvel
                property_data = {
                    "id": propertie_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": preco,
                    "tamanho": tamanho,
                    "n_quartos": n_quartos,
                    "n_banheiros": n_banheiros,
                    "n_garagem": n_garagem,
                    "rua": rua,
                    "bairro": bairro,
                    "cidade": cidade,
                    "latitude": latitude,
                    "longitude": longitude,
                }
                
                properties_count += 1
                logger.debug(f"Yielding property data: {property_data}")
                yield property_data

            # Check for duplicate page content
            if SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and len(current_page_ids) > 0:
                logger.warning(f"Stopping due to duplicate content. Found {duplicates_found} duplicates from previous page. Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids

            # Incrementar pagina para próximo yield
            page_number += 1
            logger.info(f"Moving to next page: {page_number}")

        # Se o status não for 200
        else:
            # Mostre a url atual
            logger.warning(f"Got non-200 status code ({response.status_code}) for page {page_number}")
            logger.info(f"Stopping pagination at page {page_number-1}")
            
            # Pare a função
            break 
            
    logger.info(f"Completed register scraping. Total properties processed: {properties_count}")

# Fazer função para registro de mudanças de preço dos imóveis
@dlt.resource(name="viva_real_history", write_disposition="append", primary_key="id", columns=PriceRegister)
def generate_viva_real_history(
    base_url: str = VIVA_REAL_CONFIG['base_url'],
    propertie_html_class: str = VIVA_REAL_CONFIG['property_card']['html_class'],
    propertie_html_element: str = VIVA_REAL_CONFIG['property_card']['html_element'],
    page_number: int = 1
) -> Iterable[dict]:
    logger.info("Starting Viva Real price history scraping")
    scraper = cloudscraper.create_scraper()
    history_count = 0
    previous_page_ids = set()
    
    while True:
        # Check if we've reached the maximum number of pages
        if MAX_PAGES and page_number > MAX_PAGES:
            logger.info(f"Reached maximum number of pages ({MAX_PAGES}). Stopping price history scraping.")
            break
            
        # Definir url pagina atual
        url = base_url.replace('@', str(page_number)) if '@' in base_url else base_url + f"{VIVA_REAL_CONFIG['pagination_param']}{page_number}"

        # Mostra página atual iterada
        logger.info(f"Scraping price history page {page_number} - URL: {url}")

        # Tentar pegar a response
        try:
            logger.debug(f"Sending HTTP request to {url}")
            response = scraper.get(url, allow_redirects=False)
            logger.debug(f"Received response with status code: {response.status_code}")

        # Em caso de erro, pare a função
        except HTTPError as e:
            # Mostre a url atual
            logger.error(f"HTTP Error while accessing page {page_number}: {str(e)}")
            
            # Pare a função
            break 

        # Se o status vier 200, prossiga
        if response.status_code == 200:
            # Pegar sopa de letras com o BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")

            # Pegar todos os cards de imoveis anunciados
            cards_imoveis = soup.find_all(propertie_html_element, class_=propertie_html_class)
            logger.info(f"Found {len(cards_imoveis)} property cards for price history on page {page_number}")

            current_page_ids = set()
            duplicates_found = 0

            # Iterar todos os cards de imóvel
            for i, card_imovel in enumerate(cards_imoveis):
                logger.debug(f"Processing price history for property {i+1}/{len(cards_imoveis)} on page {page_number}")
                
                # Pegar campo de preço do imovel
                preco = vivaReal.return_viva_real_preco(
                    card_imovel,
                    VIVA_REAL_CONFIG['price']
                )
                logger.debug(f"Extracted price: {preco}")

                # Pegar o campo de rua, bairro, e cidade
                rua, bairro, cidade = vivaReal.return_viva_real_endereco(
                    card_imovel,
                    VIVA_REAL_CONFIG['address']
                )
                logger.debug(f"Extracted address: {rua}, {bairro}, {cidade}")

                # Gerar id com hash md5 (usar uma junção de rua bairro e cidade)
                propertie_id = make_propertie_id(list_of_string_to_concatenate=[rua, bairro, cidade])
                logger.debug(f"Generated property ID: {propertie_id}")

                current_page_ids.add(propertie_id)
                if propertie_id in previous_page_ids:
                    duplicates_found += 1

                # Retornar o dicionários com os dados do imóvel
                history_data = {
                    "id": propertie_id,
                    "datahora": datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"),
                    "preco": preco,
                }
                
                history_count += 1
                logger.debug(f"Yielding price history data: {history_data}")
                yield history_data

            # Check for duplicate page content
            if SCRAPER_SETTINGS.get('duplicate_page_threshold', 0) > 0 and duplicates_found >= SCRAPER_SETTINGS['duplicate_page_threshold'] and len(current_page_ids) > 0:
                logger.warning(f"Stopping price history scraping due to duplicate content. Found {duplicates_found} duplicates from previous page. Threshold is {SCRAPER_SETTINGS['duplicate_page_threshold']}.")
                break

            previous_page_ids = current_page_ids

            # Incrementar pagina para próximo yield
            page_number += 1
            logger.info(f"Moving to next price history page: {page_number}")

        # Se o status não for 200
        else:
            # Mostre a url atual
            logger.warning(f"Got non-200 status code ({response.status_code}) for price history page {page_number}")
            logger.info(f"Stopping price history pagination at page {page_number-1}")
            
            # Pare a função
            break
            
    logger.info(f"Completed price history scraping. Total history records processed: {history_count}")

# Fazer função juntando os recursos do viva real
@dlt.source
def generate_viva_real():
    logger.info("Registering Viva Real resources")
    # yield resources
    yield generate_viva_real_register
    yield generate_viva_real_history

# Fazer pipeline DLT
logger.info("Creating Viva Real DLT pipeline")
pipeline = dlt.pipeline(
    # Nome do pipeline
    pipeline_name="kodomiya",

    # Nome do schema dentro do DB
    dataset_name="kodomiya_viva_real",

    # Destino duckdb
    destination=dlt.destinations.duckdb(fr"{DATABASE_CONFIG['path']}/kodomiya.duckdb"),
)

# Executar pipeline com o source
logger.info("Running Viva Real pipeline")
try:
    pipeline_result = pipeline.run(generate_viva_real())
    logger.info(f"Pipeline completed successfully: {pipeline_result}")

except Exception as e:
    logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)

logger.info("Viva Real pipeline execution finished")
