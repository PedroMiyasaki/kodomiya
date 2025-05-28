# Import pydantic
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

# Fazer classe imovel
class Register(BaseModel):
    id: str
    datahora: datetime

# Contruir classe schema de imovel register
class ImovelRegister(Register):
    preco: Optional[float]
    tamanho: Optional[float]
    n_quartos: int 
    n_banheiros:int
    n_garagem: int
    rua: str
    bairro: Optional[str]
    cidade: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

# Contruir classe schema de imovel History
class PriceRegister(Register):
    preco: Optional[float]