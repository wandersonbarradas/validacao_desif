from sqlalchemy import Column, Integer, String
from app.db import Base


class ProdutoServico(Base):
    __tablename__ = 'outros_produtos_servicos'

    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String, index=True)
    descricao = Column(String, index=True)
    descricao_complementar_obrigatoria = Column(String)
