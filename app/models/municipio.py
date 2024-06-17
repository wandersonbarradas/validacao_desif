from sqlalchemy import Column, Integer, String
from app.db import Base


class Municipio(Base):
    __tablename__ = 'municipios'

    id = Column(Integer, primary_key=True, index=True)
    id_estado = Column(Integer, index=True)
    nome = Column(String, index=True)
    slug = Column(String, index=True)
    cod_ibge = Column(String, index=True)
