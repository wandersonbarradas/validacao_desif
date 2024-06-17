from sqlalchemy import Column, Integer, String
from app.db import Base


class CodigoTributacao(Base):
    __tablename__ = 'codigos_tributacao'

    id = Column(Integer, primary_key=True, index=True)
    id_item_servico_nacional = Column(Integer, index=True)
    codigo = Column(String, index=True)
    codigo_pontuacao = Column(String, index=True)
    descricao = Column(String, index=True)
