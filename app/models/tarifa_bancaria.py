from sqlalchemy import Column, Integer, String
from app.db import Base


class TarifaBancaria(Base):
    __tablename__ = 'tarifas_bancarias'

    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String, index=True)
    descricao = Column(String, index=True)
    periodicidade = Column(String, index=True)
