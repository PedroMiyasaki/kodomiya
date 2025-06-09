import json
import time
from geopy.geocoders import Nominatim
import logging
import os

# --- Project Root Setup ---
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_coordinates(points_of_interest):
    """
    Geocodes a list of points of interest in Curitiba and returns their coordinates.

    Args:
        points_of_interest (list): A list of dictionaries, each containing the point name and query string.

    Returns:
        list: A list of dictionaries, each containing the point name, latitude, and longitude.
    """
    # Using a specific user_agent is a good practice as per Nominatim's usage policy
    geolocator = Nominatim(user_agent="kodomiya_poi_geocoder_v1")
    results = []

    for point in points_of_interest:
        point_name = point['point_name']
        query = point['query']
        
        logging.info(f"Geocoding: '{query}'...")
        
        try:
            # Nominatim usage policy requires a delay of at least 1 second per request
            time.sleep(1)
            # We use the specific query provided for better accuracy
            location = geolocator.geocode(query, country_codes=['BR'])
            
            if location:
                results.append({
                    "point_name": point_name,
                    "latitude": location.latitude,
                    "longitude": location.longitude
                })
                logging.info(f"  -> Found: Lat={location.latitude}, Lon={location.longitude}")
            else:
                results.append({
                    "point_name": point_name,
                    "latitude": None,
                    "longitude": None
                })
                logging.warning(f"  -> Not found for '{point_name}'.")
        
        except Exception as e:
            logging.error(f"An error occurred while geocoding '{point_name}': {e}")
            results.append({
                "point_name": point_name,
                "latitude": None,
                "longitude": None
            })

    return results

if __name__ == "__main__":
    # List of points of interest with improved, specific query strings
    poi_list = [
        {"point_name": "Parque Barigui", "query": "Parque Barigui, Santo Inácio, Curitiba"},
        {"point_name": "Jardim Botânico de Curitiba", "query": "Jardim Botânico de Curitiba, Jardim Botânico, Curitiba"},
        {"point_name": "Shopping Mueller", "query": "Shopping Mueller, Centro Cívico, Curitiba"},
        {"point_name": "Pátio Batel", "query": "Pátio Batel, Batel, Curitiba"},
        {"point_name": "Universidade Federal do Paraná (UFPR) - Reitoria", "query": "Reitoria da Universidade Federal do Paraná, Praça Santos Andrade, Curitiba"},
        {"point_name": "Pontifícia Universidade Católica do Paraná (PUCPR)", "query": "Pontifícia Universidade Católica do Paraná, Prado Velho, Curitiba"},
        {"point_name": "Centro Cívico (Government Buildings)", "query": "Palácio Iguaçu, Centro Cívico, Curitiba"},
        {"point_name": "Hospital de Clínicas da UFPR", "query": "Hospital de Clínicas da UFPR, Alto da Glória, Curitiba"},
        {"point_name": "Rodoferroviária de Curitiba", "query": "Rodoferroviária de Curitiba, Jardim Botânico, Curitiba"},
        {"point_name": "Ópera de Arame", "query": "Ópera de Arame, Abranches, Curitiba"},
        {"point_name": "Museu Oscar Niemeyer (MON)", "query": "Museu Oscar Niemeyer, Centro Cívico, Curitiba"},
        {"point_name": "Largo da Ordem (Historic Center)", "query": "Largo da Ordem, São Francisco, Curitiba"},
        {"point_name": "Estádio Couto Pereira (Coritiba FC)", "query": "Estádio Major Antônio Couto Pereira, Alto da Glória, Curitiba"},
        {"point_name": "Ligga Arena (Athletico-PR)", "query": "Estádio Joaquim Américo Guimarães, Água Verde, Curitiba"},
        {"point_name": "Mercado Municipal de Curitiba", "query": "Mercado Municipal de Curitiba, Centro, Curitiba"},
        {"point_name": "Santa Felicidade (Gastronomic Hub)", "query": "Avenida Manoel Ribas, Santa Felicidade, Curitiba"},
        {"point_name": "Rua 24 Horas", "query": "Rua 24 Horas, Centro, Curitiba"},
        {"point_name": "Parolin (General Area)", "query": "Bairro Parolin, Curitiba"},
        {"point_name": "Cidade Industrial de Curitiba (CIC)", "query": "Cidade Industrial de Curitiba, Curitiba"},
        {"point_name": "Aterro Sanitário da Caximba", "query": "Aterro Sanitário da Caximba, Caximba, Curitiba"},
        {"point_name": "Complexo Penitenciário de Piraquara", "query": "Complexo Penitenciário de Piraquara, Piraquara, PR"},
        {"point_name": "Rua da Cidadania CIC", "query": "Rua da Cidadania CIC, Cidade Industrial de Curitiba"},
        {"point_name": "Terminal Pinheirinho", "query": "Terminal do Pinheirinho, Pinheirinho, Curitiba"},
        {"point_name": "Shopping Estação", "query": "Shopping Estação, Rebouças, Curitiba"},
        {"point_name": "Shopping Palladium", "query": "Shopping Palladium, Portão, Curitiba"},
        {"point_name": "Shopping Curitiba", "query": "Shopping Curitiba, Batel, Curitiba"},
        {"point_name": "ParkShoppingBarigüi", "query": "ParkShoppingBarigüi, Mossunguê, Curitiba"},
        {"point_name": "Parque Tanguá", "query": "Parque Tanguá, Pilarzinho, Curitiba"},
        {"point_name": "Parque Tingui", "query": "Parque Tingui, São João, Curitiba"},
        {"point_name": "Bosque do Papa João Paulo II", "query": "Bosque do Papa, Centro Cívico, Curitiba"},
        {"point_name": "Praça do Japão", "query": "Praça do Japão, Batel, Curitiba"},
        {"point_name": "Praça da Espanha", "query": "Praça da Espanha, Bigorrilho, Curitiba"},
        {"point_name": "Terminal do Cabral", "query": "Terminal do Cabral, Cabral, Curitiba"},
        {"point_name": "Terminal do Portão", "query": "Terminal do Portão, Portão, Curitiba"},
        {"point_name": "Terminal Campina do Siqueira", "query": "Terminal Campina do Siqueira, Campina do Siqueira, Curitiba"},
        {"point_name": "Rua XV de Novembro (Calçadão)", "query": "Calçadão da Rua XV de Novembro, Centro, Curitiba"},
        {"point_name": "Hospital Pequeno Príncipe", "query": "Hospital Pequeno Príncipe, Água Verde, Curitiba"},
        {"point_name": "Hospital Vita Batel", "query": "Hospital Vita, Batel, Curitiba"},
        {"point_name": "Hospital Marcelino Champagnat", "query": "Hospital Marcelino Champagnat, Cristo Rei, Curitiba"},
        {"point_name": "UTFPR - Câmpus Curitiba", "query": "UTFPR Câmpus Curitiba, Rebouças, Curitiba"},
        {"point_name": "Unicuritiba", "query": "Unicuritiba, Rebouças, Curitiba"},
        {"point_name": "FAE Centro Universitário", "query": "FAE Business School, Centro, Curitiba"},
        {"point_name": "Teatro Guaíra", "query": "Teatro Guaíra, Centro, Curitiba"},
        {"point_name": "Pedreira Paulo Leminski", "query": "Pedreira Paulo Leminski, Abranches, Curitiba"},
        {"point_name": "Cemitério Municipal São Francisco de Paula", "query": "Cemitério Municipal São Francisco de Paula, São Francisco, Curitiba"}
    ]

    coordinates_data = get_coordinates(poi_list)

    # Save to a JSON file in the project root for easy access
    output_filename = os.path.join(_PROJECT_ROOT, 'points_of_interest.json')
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(coordinates_data, f, indent=4, ensure_ascii=False)
        
    logging.info(f"\nGeocoding complete. Data saved to '{output_filename}'")
    
    # Print the JSON to the console as well for immediate feedback
    print("\n--- JSON Output ---")
    print(json.dumps(coordinates_data, indent=4, ensure_ascii=False))