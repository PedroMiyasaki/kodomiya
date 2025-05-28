# Imports online
from unidecode import unidecode
from hashlib import md5

# Fazer função que só deixa numeros em uma string
def return_only_alphanumeric_part(word):
    # Retornar apenar parte numérica da palavra
    return "".join([c for c in word if c.isnumeric()])

# Fazer função de achar palavra em uma sentença e retornar palavra
def return_word_founded_in_sentence(sentence, list_of_words, lower_sentence=True):
    # Aplicar unidecode na frase
    sentence = unidecode(sentence)
    
    # Converter frase para lower se lower_sentence true
    if lower_sentence:
        sentence = sentence.lower()

    # Iterar a lista de palavras
    for word in list_of_words:
        if word in sentence:
            return word
        
    # Do contrário, retorno Nulo
    return None

# Fazer função para geração de id com sequencia de plaavars
def make_propertie_id(list_of_string_to_concatenate):
    # Retirar nulos da lista caso eles existam
    cleaned_list_of_string_to_concatenate = [i for i in list_of_string_to_concatenate if i is not None]

    # Montar string para o id
    id_string = "".join(cleaned_list_of_string_to_concatenate)

    # Passar limpeza no id
    id_string = unidecode(id_string.lower().strip().replace(" ", "").replace(",", ""))

    # Gerar hash md5 com string
    return md5(id_string.encode("utf-8")).hexdigest()