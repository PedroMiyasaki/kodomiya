import logging

# Imports offline
from .common.common_functions import return_word_founded_in_sentence, return_only_alphanumeric_part
from .common.common_objects import neighborhood_names, city_names
from datetime import datetime
from typing import Optional
import requests
from bs4 import BeautifulSoup
from requests import HTTPError

logger = logging.getLogger(__name__)

# Fazer classe de funções da fonte chaves na mão
class chavesNaMao():
    # Fazer função para retornar o preço no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_preco(propertie_card, price_tag, price_class_name, price_value_tag, **kwargs):
        # Buscar preço no card da propriedade
        try:
            price_element = propertie_card.find(price_tag, class_=price_class_name)
            price_text = price_element.find(price_value_tag).text
            price_text = price_text.replace("R$ ", "").replace(".", "_")
            
            # Tentar retornar o preço convertido para float
            return float(price_text)
        
        except (AttributeError, ValueError, IndexError) as e:
            return None
        
    # Fazer função para retornar o tamanho do imóvel no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_tamanho(propertie_card, tag, class_name, index=0, split_text=None, **kwargs):
        # Buscar pelo tamanho
        try:
            # Find all elements with the specified tag and class
            tamanho_elements = propertie_card.find_all(tag, class_=class_name)
            
            if tamanho_elements and len(tamanho_elements) > index:
                tamanho_text = tamanho_elements[index].text.strip()
                
                # Split by the specified text if provided
                if split_text and split_text in tamanho_text:
                    tamanho = tamanho_text.split(split_text)[0].strip()
                else:
                    tamanho = tamanho_text
                
                # Retornar tamanho convertido para inteiro
                return int(tamanho)
            return None
        except (ValueError, IndexError):
            return None

    # Fazer função para retornar o n de quartos no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_n_quartos(propertie_card, tag, class_name, search_text, **kwargs):
        try:
            # Procurar nos elementos com a tag e classe especificadas que contém o texto de busca
            room_elements = [p for p in propertie_card.find_all(tag, class_=class_name) 
                           if search_text in p.text]
            
            if room_elements:
                # Extrair somente o número dos quartos
                room_text = room_elements[0].text.strip()
                # Pegar o número antes do texto de busca
                room_number = room_text.split(search_text)[0].strip()
                return int(return_only_alphanumeric_part(room_number))
            return 0
        except (ValueError, IndexError):
            return 0
        
    # Fazer função para retornar o n de banheiros no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_n_banheiros(propertie_card, tag, class_name, search_text, **kwargs):
        try:
            # Procurar nos elementos com a tag e classe especificadas que contém o texto de busca
            bathroom_elements = [p for p in propertie_card.find_all(tag, class_=class_name) 
                               if search_text in p.text]
            
            if bathroom_elements:
                # Extrair somente o número dos banheiros
                bathroom_text = bathroom_elements[0].text.strip()
                # Pegar o número antes do texto de busca
                bathroom_number = bathroom_text.split(search_text)[0].strip()
                return int(return_only_alphanumeric_part(bathroom_number))
            return 0
        except (ValueError, IndexError):
            return 0
        
    # Fazer função para retornar o n de garagems no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_n_vagas_garagem(propertie_card, tag, class_name, search_text, **kwargs):
        try:
            # Procurar nos elementos com a tag e classe especificadas que contém o texto de busca
            garage_elements = [p for p in propertie_card.find_all(tag, class_=class_name) 
                             if search_text in p.text]
            
            if garage_elements:
                # Extrair somente o número das garagens
                garage_text = garage_elements[0].text.strip()
                # Pegar o número antes do texto de busca
                garage_number = garage_text.split(search_text)[0].strip()
                return int(return_only_alphanumeric_part(garage_number))
            return 0
        except (ValueError, IndexError):
            return 0
        
    # Fazer função para retornar o endereço no site chaves na mao
    @staticmethod
    def return_chaves_na_mao_endereco(propertie_card, main_tag, class_name, rua_tag, rua_index, bairro_cidade_tag, bairro_cidade_index, **kwargs):
        try:
            # Buscar a tag de endereço
            address_element = propertie_card.find(main_tag, class_=class_name)
            
            if address_element:
                # Extrair rua do endereco (primeiro elemento p)
                p_elements = address_element.find_all(rua_tag)
                rua = p_elements[rua_index].text.strip() if len(p_elements) > rua_index else ""
                
                # Extrair bairro e cidade do segundo elemento p
                bairro_e_cidade = p_elements[bairro_cidade_index].text.strip() if len(p_elements) > bairro_cidade_index else ""
                
                # Extrair bairro do endereço
                bairro = return_word_founded_in_sentence(bairro_e_cidade, neighborhood_names)
                
                # Extrair cidade do endereço
                cidade = return_word_founded_in_sentence(bairro_e_cidade, city_names)
                
                return rua, bairro, cidade
            
            return "", "", ""
        except (IndexError, AttributeError):
            return "", "", ""

# Fazer classe da fonte zap imoveis
class zapImoveis():
    # Fazer função para retornar o preço no site zap imoveis
    @staticmethod
    def return_zap_imoveis_preco(propertie_card, price_config):
        # Buscar preço no card da propriedade
        try:
            price_element = propertie_card.find(price_config['tag'], attrs={'data-cy': price_config['data_cy']})
            if price_element:
                price_text = price_element.find(price_config['child_tag']).text
                if price_config.get('replace_dots'):
                    price_text = price_text.replace(".", "") # Remove dots for thousands separator
                price_text = price_text.replace("R$ ", "").replace(",", ".") # Replace comma for decimal
                return float(price_text)
        except (AttributeError, ValueError, IndexError) as e:
            return None
        return None

    # Fazer função para retornar endereco no site zap imoveis
    @staticmethod
    def return_zap_imoveis_endereco(propertie_card, address_config):
        try:
            # Extrair rua do endereco
            street_element = propertie_card.find(address_config['street_tag'], attrs={'data-cy': address_config['street_data_cy']})
            rua = street_element.text.strip() if street_element else ""

            # Extrair bairro e cidade da tag de localização
            location_element = propertie_card.find(address_config['location_tag'], attrs={'data-cy': address_config['location_data_cy']})
            bairro_e_cidade_text = ""
            if location_element:
                # The city and neighborhood are after the span, so we get the last part of the string contents
                # Example: <span...>Casa para comprar em </span>Santa Cândida, Curitiba
                # We want "Santa Cândida, Curitiba"
                if location_element.span: # Check if span exists
                    location_element.span.extract() # Remove the span to get the remaining text
                bairro_e_cidade_text = location_element.text.strip()
            
            # Extrair bairro do endereço
            bairro = return_word_founded_in_sentence(bairro_e_cidade_text, neighborhood_names)

            # Extrair cidade do endereço
            cidade = return_word_founded_in_sentence(bairro_e_cidade_text, city_names)

            # Retornar todos os componentes do endereço
            return rua, bairro, cidade
        except (AttributeError, IndexError) as e:
            return "", "", ""

    # Fazer função para retornar o tamanho do imovel no site zap imoveis
    @staticmethod
    def return_zap_imoveis_tamanho(propertie_card, size_config):
        try:
            parent_element = propertie_card.find(size_config['parent_tag'], attrs={'data-cy': size_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(size_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    tamanho_text = value_element.text.strip()
                    if size_config['split_text'] in tamanho_text:
                        tamanho = tamanho_text.split(size_config['split_text'])[0].strip()
                        return int(tamanho)
        except (AttributeError, ValueError, IndexError) as e:
            return None
        return None

    # Fazer função para retornar o numero de quartos site zap imoveis
    @staticmethod
    def return_zap_imoveis_n_quartos(propertie_card, rooms_config):
        try:
            parent_element = propertie_card.find(rooms_config['parent_tag'], attrs={'data-cy': rooms_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(rooms_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    quartos_text = value_element.text.strip()
                    return int(quartos_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0 if not found or error
        return 0

    # Fazer função para retornar o numero de banheiros site zap imoveis
    @staticmethod
    def return_zap_imoveis_n_banheiros(propertie_card, bathrooms_config):
        try:
            parent_element = propertie_card.find(bathrooms_config['parent_tag'], attrs={'data-cy': bathrooms_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(bathrooms_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    banheiros_text = value_element.text.strip()
                    return int(banheiros_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0
        return 0

     # Fazer função para retornar o numero de vagas de garagem site zap imoveis
    @staticmethod
    def return_zap_imoveis_n_vagas_garagem(propertie_card, parking_config):
        try:
            parent_element = propertie_card.find(parking_config['parent_tag'], attrs={'data-cy': parking_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(parking_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    garagem_text = value_element.text.strip()
                    return int(garagem_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0
        return 0

# Fazer classe da fonte viva real
class vivaReal():
    # Fazer função para retornar o preço no site viva real
    @staticmethod
    def return_viva_real_preco(propertie_card, price_config):
        # Buscar preço no card da propriedade
        try:
            price_element = propertie_card.find(price_config['tag'], attrs={'data-cy': price_config['data_cy']})
            if price_element:
                # The main price is in the first <p> tag
                price_text_element = price_element.find_all(price_config['child_tag'], recursive=False)[0]
                price_text = price_text_element.text
                if price_config.get('replace_dots'):
                    price_text = price_text.replace(".", "") # Remove dots for thousands separator
                price_text = price_text.replace("R$ ", "").replace(",", ".") # Replace comma for decimal
                return float(price_text)
        except (AttributeError, ValueError, IndexError, TypeError) as e:
            return None
        return None

    # Fazer função para retornar o tamanho no site viva real
    @staticmethod
    def return_viva_real_tamanho(propertie_card, size_config):
        try:
            parent_element = propertie_card.find(size_config['parent_tag'], attrs={'data-cy': size_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(size_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    tamanho_text = value_element.text.strip()
                    if size_config['split_text'] in tamanho_text:
                        tamanho = tamanho_text.split(size_config['split_text'])[0].strip()
                        return int(tamanho)
        except (AttributeError, ValueError, IndexError) as e:
            return None
        return None
    
    # Fazer função de retorno de numero de quartos no site viva real
    @staticmethod
    def return_viva_real_n_quartos(propertie_card, rooms_config):
        try:
            parent_element = propertie_card.find(rooms_config['parent_tag'], attrs={'data-cy': rooms_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(rooms_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    quartos_text = value_element.text.strip()
                    return int(quartos_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0 if not found or error
        return 0

    # Fazer função de retorno de numero de banheiros no site viva real
    @staticmethod
    def return_viva_real_n_banheiros(propertie_card, bathrooms_config):
        try:
            parent_element = propertie_card.find(bathrooms_config['parent_tag'], attrs={'data-cy': bathrooms_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(bathrooms_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    banheiros_text = value_element.text.strip()
                    return int(banheiros_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0
        return 0

    # Fazer função de retorno de numero de vagas de garagem no site viva real
    @staticmethod
    def return_viva_real_n_vagas_garagem(propertie_card, parking_config):
        try:
            parent_element = propertie_card.find(parking_config['parent_tag'], attrs={'data-cy': parking_config['parent_data_cy']})
            if parent_element:
                value_element = parent_element.find(parking_config['value_tag'])
                if value_element:
                    # Remove SVG and span before getting text
                    if value_element.svg:
                        value_element.svg.extract()
                    if value_element.span:
                        value_element.span.extract()
                    garagem_text = value_element.text.strip()
                    return int(garagem_text)
        except (AttributeError, ValueError, IndexError) as e:
            return 0 # Default to 0
        return 0

    # Fazer função de retorno de endereço no site viva real
    @staticmethod
    def return_viva_real_endereco(propertie_card, address_config):
        try:
            # Extrair rua do endereco
            street_element = propertie_card.find(address_config['street_tag'], attrs={'data-cy': address_config['street_data_cy']})
            rua = street_element.text.strip() if street_element else ""

            # Extrair bairro e cidade da tag de localização
            location_element = propertie_card.find(address_config['location_tag'], attrs={'data-cy': address_config['location_data_cy']})
            bairro_e_cidade_text = ""
            if location_element:
                # The city and neighborhood are after the span
                if location_element.span:
                    location_element.span.extract() 
                bairro_e_cidade_text = location_element.text.strip()
            
            bairro = return_word_founded_in_sentence(bairro_e_cidade_text, neighborhood_names)
            cidade = return_word_founded_in_sentence(bairro_e_cidade_text, city_names)

            return rua, bairro, cidade
        except (AttributeError, IndexError) as e:
            return "", "", ""

# Fazer classe da fonte leilao imovel
class leilaoImovel():
    @staticmethod
    def _parse_praca_datetime(date_str: str, time_str: str) -> Optional[datetime]:
        """Helper to parse date and time from praça string."""
        try:
            # Expected date format: DD/MM/YYYY, time format: HH:MM
            return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
        except ValueError:
            return None

    @staticmethod
    def _parse_praca_price(price_str: str) -> Optional[float]:
        """Helper to parse price from praça string."""
        try:
            return float(price_str.replace("R$ ", "").replace(".", "").replace(",", "."))
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def return_leilao_imovel_link_detalhes(propertie_card, leilao_config_dict: dict):
        try:
            # The propertie_card is now div.place-box.
            # We need to find the main <a> tag within it.

            # Strategy 1: Find an 'a' tag that contains a 'div' with the price_details main_container_class
            all_a_tags = propertie_card.find_all("a", href=True)
            for a_tag in all_a_tags:
                price_details_config = leilao_config_dict.get('price_details', {})
                main_price_container_class = price_details_config.get('main_container_class')
                if main_price_container_class and a_tag.find(class_=main_price_container_class):
                    link = a_tag.get("href")
                    if link and not link.startswith("http"):
                        return f"https://www.leilaoimovel.com.br{link}"
                    return link

            # Fallback Strategy 2: if no specific link found, try to find first 'a' with class Link_Redirecter
            # This was the original selector for the <a> tag when it was the property_card
            link_element = propertie_card.find("a", class_="Link_Redirecter", href=True)
            if link_element:
                 link = link_element.get("href")
                 if link and not link.startswith("http"):
                     return f"https://www.leilaoimovel.com.br{link}"
                 return link
            
            # Fallback Strategy 3: Try to find any <a> tag that is a direct child and has an href.
            # This may be too simplistic if structure is more nested.
            direct_a_tags = propertie_card.find_all("a", href=True, recursive=False)
            if direct_a_tags:
                link = direct_a_tags[0].get("href")
                if link and not link.startswith("http"):
                    return f"https://www.leilaoimovel.com.br{link}"
                return link


            # Previous fallback based on onclick, likely not applicable to div.place-box but kept as last resort if structure changes
            # onclick_attr = propertie_card.get('onclick') # This would be on the div.place-box
            # if onclick_attr:
            #     match = re.search(r"href='([^']+)'", onclick_attr) # This pattern might be too specific
            #     if match:
            #         link = match.group(1)
            #         if link and not link.startswith("http"):
            #             return f"https://www.leilaoimovel.com.br{link}"
            #         return link
            
            logger.warning(f"Could not find details page link in property card: {propertie_card.prettify()[:500]}")
            return None
        except Exception as e:
            logger.error(f"Error parsing link_detalhes: {e} for card {propertie_card.prettify()[:200]}")
            return None

    @staticmethod
    def return_leilao_imovel_praca_info(propertie_card, auction_config):
        preco_primeira_praca = None
        data_primeira_praca = None
        preco_segunda_praca = None
        data_segunda_praca = None
        preco_atual = None

        try:
            # Get current displayed price first (usually the effective one)
            price_container_div = propertie_card.find(class_=auction_config['price_details']['main_container_class'])
            if price_container_div:
                current_price_span = price_container_div.find(class_=auction_config['price_details']['current_price_class'])
                if current_price_span:
                    preco_atual = leilaoImovel._parse_praca_price(current_price_span.text.strip())

            infos_container = propertie_card.find(class_=auction_config['auction_info']['container_class'])
            if not infos_container:
                return preco_primeira_praca, data_primeira_praca, preco_segunda_praca, data_segunda_praca, preco_atual

            info_spans = infos_container.find_all(auction_config['auction_info']['info_span_selector'])

            for span in info_spans:
                text_content = span.get_text(separator=" ", strip=True)
                price_element = span.find('b', class_='price') # Price within the span
                strikethrough_price_element = span.find('s', class_='price') # Striked price

                # Determine if the info is striked out (already passed)
                is_striked = span.find(auction_config['auction_info'].get('strikethrough_tag', 's')) is not None

                current_span_price = None
                if price_element:
                    current_span_price = leilaoImovel._parse_praca_price(price_element.text.strip())
                elif strikethrough_price_element: # Check for striked-out price if normal not found
                     current_span_price = leilaoImovel._parse_praca_price(strikethrough_price_element.text.strip())

                if auction_config['auction_info']['praca_unica_text'] in text_content:
                    date_time_part = text_content.split(auction_config['auction_info']['praca_unica_text'])[1].strip().split(' ')
                    if len(date_time_part) >= 2:
                        data_primeira_praca = leilaoImovel._parse_praca_datetime(date_time_part[0], date_time_part[1])
                        preco_primeira_praca = current_span_price
                        if not is_striked and preco_atual is None: # Update preco_atual if not striked AND not already set
                            preco_atual = preco_primeira_praca
                    break # Praça única, no need to check further

                elif auction_config['auction_info']['primeira_praca_text'] in text_content:
                    date_time_part = text_content.split(auction_config['auction_info']['primeira_praca_text'])[1].strip().split(' ')
                    if len(date_time_part) >= 2:
                        data_primeira_praca = leilaoImovel._parse_praca_datetime(date_time_part[0], date_time_part[1])
                        preco_primeira_praca = current_span_price
                        if not is_striked and preco_atual is None: # Prioritize non-striked if preco_atual not set by discount-price
                            preco_atual = preco_primeira_praca
                
                elif auction_config['auction_info']['segunda_praca_text'] in text_content:
                    date_time_part = text_content.split(auction_config['auction_info']['segunda_praca_text'])[1].strip().split(' ')
                    if len(date_time_part) >= 2:
                        data_segunda_praca = leilaoImovel._parse_praca_datetime(date_time_part[0], date_time_part[1])
                        preco_segunda_praca = current_span_price
                        if not is_striked and preco_atual is None: # If second praça is active and preco_atual not set, it's the current price
                           preco_atual = preco_segunda_praca 
            
            # If preco_atual is still None, it might be a "Venda Direta" or similar without explicit praça pricing in infos.
            # The initial current_price_element parsing should handle this.
            # If only one praça_price is found, and it's not a "praça única" scenario from above, assign it to preco_atual if preco_atual is still None.
            if preco_atual is None:
                if preco_primeira_praca and data_primeira_praca and not data_segunda_praca:
                    preco_atual = preco_primeira_praca
                elif preco_segunda_praca and data_segunda_praca:
                     preco_atual = preco_segunda_praca


        except (AttributeError, IndexError, TypeError) as e:
            # Log error or handle, for now, pass
            pass 
        
        return preco_primeira_praca, data_primeira_praca, preco_segunda_praca, data_segunda_praca, preco_atual

    @staticmethod
    def return_leilao_imovel_endereco(propertie_card, address_config):
        rua, bairro, cidade = "", "", ""
        try:
            address_container = propertie_card.find(class_=address_config.get('container_class'))
            if not address_container:
                return "", "", ""

            p_tag = address_container.find(address_config.get('text_container_tag'))
            if not p_tag:
                return "", "", ""

            # Attempt to get city from title first, as it's more reliable
            title_b_tag = p_tag.find('b')
            if title_b_tag and 'em Leilão em' in title_b_tag.text:
                try:
                    cidade = title_b_tag.text.split('em Leilão em')[1].split('/')[0].strip()
                except IndexError:
                    pass

            span_tag = p_tag.find('span')
            if not span_tag:
                # If city was found, return that, otherwise Curitiba as default
                return "", "", cidade if cidade else "Curitiba"
            
            full_address_text = ' '.join(span_tag.text.strip().split()) # Normalize spaces

            # The address string is consistently structured with "- CEP:" separating address from location info.
            if " - CEP:" in full_address_text:
                address_details, location_details = full_address_text.split(" - CEP:", 1)
            else:
                address_details = full_address_text
                location_details = ""

            # Extract city from the location part if not already found from the title
            if not cidade and "," in location_details:
                try:
                    # e.g. "81580-010, CURITIBA - PARANA" -> "CURITIBA"
                    cidade = location_details.split(',')[1].split(' - ')[0].strip()
                except IndexError:
                    pass
            
            # Default city if still not found
            if not cidade:
                cidade = "Curitiba"

            # The first part of the address details, before any comma, is the street.
            rua = address_details.split(',')[0].strip()

            # Use the list of known neighborhoods to find the bairro in the address string.
            # This is more robust than relying on position alone, which was causing errors.
            # It correctly handles cases where address complements are mistaken for neighborhoods.
            bairro = return_word_founded_in_sentence(address_details, neighborhood_names)

            return rua.strip(), bairro.strip() if bairro else "", cidade.strip()

        except Exception as e:
            logger.error(f"Error parsing leilao_imovel address: {e}", exc_info=True)
            return "", "", ""

    @staticmethod
    def return_leilao_imovel_details_page_info(details_page_url: str, details_config: dict, scraper_instance):
        area_util = None
        area_terreno = None
        aceita_financiamento = None
        aceita_fgts = None
        n_garagem = None
        n_quartos = None

        # Extract relevant sub-configs for clarity
        size_config = details_config.get('size', {})
        financing_config = details_config.get('financing_fgts_details', {})
        parking_config = details_config.get('parking_details', {})
        room_config = details_config.get('room_details', {})

        if not details_page_url:
            return {"area_util": area_util, "area_terreno": area_terreno, "aceita_financiamento": aceita_financiamento, "aceita_fgts": aceita_fgts, "n_garagem": n_garagem, "n_quartos": n_quartos}
        try:
            response = scraper_instance.get(details_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            # Extract Area Util and Area Terreno
            details_container_size = soup.find(class_=size_config.get('container_class'))
            if details_container_size:
                detail_divs = details_container_size.find_all(class_=size_config.get('detail_div_class'))
                for div in detail_divs:
                    p_tag = div.find('p')
                    if p_tag:
                        area_text_label = p_tag.text.strip()
                        value_span = div.find(size_config.get('value_span_selector'))
                        if value_span:
                            tamanho_text = value_span.text.strip()
                            if size_config.get('split_text', ' m²') in tamanho_text:
                                # Clean the string: remove thousand separators (.), then replace decimal comma (,) with dot (.)
                                tamanho_str = tamanho_text.split(size_config.get('split_text', ' m²'))[0].strip().replace('.', '').replace(',','.')
                                try:
                                    parsed_float = float(tamanho_str)
                                    # Using get with a default for area_text_identifier as it might not always be present
                                    if size_config.get('area_text_identifier_util', 'Área Útil:') in area_text_label:
                                        area_util = parsed_float
                                    elif size_config.get('area_text_identifier_terreno', 'Área Terreno:') in area_text_label:
                                        area_terreno = parsed_float
                                
                                except ValueError:
                                    print(f"Error parsing float: {tamanho_text}")

            # Extract Financiamento and FGTS info
            info_divs = soup.find_all('div', class_=financing_config.get('info_div_class'))
            for info_div in info_divs:
                p_tag = info_div.find('p')
                if p_tag:
                    text_content = p_tag.get_text(strip=True)
                    if financing_config.get('text_rejects_financing') in text_content:
                        aceita_financiamento = False
                    elif financing_config.get('text_accepts_financing') in text_content:
                        aceita_financiamento = True
                    
                    if financing_config.get('text_accepts_fgts') in text_content:
                        aceita_fgts = True
                    elif financing_config.get('text_rejects_fgts') in text_content:
                        aceita_fgts = False
            
            # Extract Parking Spaces info
            icon_divs = soup.find_all('div', class_=parking_config.get('icon_div_class'))
            for icon_div in icon_divs:
                img_tag = icon_div.find('img', alt=parking_config.get('img_alt_text'))
                # Could also check for title or src contains parking_config.get('img_src_contains')
                if img_tag:
                    span_tag = icon_div.find('span') # Assuming span is direct child or first span
                    if span_tag:
                        try:
                            n_garagem = int(span_tag.text.strip())
                            break # Found parking info
                        except ValueError:
                            logger.warning(f"Could not parse parking spaces int from '{span_tag.text.strip()}'")

            # Extract Room count info
            icon_divs_rooms = soup.find_all('div', class_=room_config.get('icon_div_class'))
            for icon_div_room in icon_divs_rooms:
                img_tag_room = icon_div_room.find('img', alt=room_config.get('img_alt_text'))
                if img_tag_room:
                    span_tag_room = icon_div_room.find('span')
                    if span_tag_room:
                        try:
                            n_quartos = int(span_tag_room.text.strip())
                            break # Found room info
                        except ValueError:
                            logger.warning(f"Could not parse room count int from '{span_tag_room.text.strip()}'")
            
            return {"area_util": area_util, "area_terreno": area_terreno, "aceita_financiamento": aceita_financiamento, "aceita_fgts": aceita_fgts, "n_garagem": n_garagem, "n_quartos": n_quartos}

        except (requests.exceptions.RequestException, HTTPError, AttributeError, ValueError, IndexError) as e:
            logger.error(f"Error fetching/parsing details from {details_page_url}: {e}")
            return {"area_util": area_util, "area_terreno": area_terreno, "aceita_financiamento": aceita_financiamento, "aceita_fgts": aceita_fgts, "n_garagem": n_garagem, "n_quartos": n_quartos}