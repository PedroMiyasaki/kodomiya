# Imports offline
from .common.common_functions import return_word_founded_in_sentence, return_only_alphanumeric_part
from .common.common_objects import neighborhood_names, city_names

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