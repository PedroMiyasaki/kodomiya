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

# Auction property schemas
class LeilaoImovelRegister(Register):
    preco_primeira_praca: Optional[float]
    data_primeira_praca: Optional[datetime]
    preco_segunda_praca: Optional[float]
    data_segunda_praca: Optional[datetime]
    preco_atual: float
    area_util: Optional[float]
    area_terreno: Optional[float]
    rua: str
    bairro: str
    cidade: str
    latitude: Optional[float]
    longitude: Optional[float]
    link_detalhes: str
    aceita_financiamento: Optional[bool]
    aceita_fgts: Optional[bool]
    n_garagem: Optional[int]
    n_quartos: Optional[int]

class LeilaoImovelHistory(Register):
    preco_primeira_praca: Optional[float]
    data_primeira_praca: Optional[datetime]
    preco_segunda_praca: Optional[float]
    data_segunda_praca: Optional[datetime]
    preco_atual: float