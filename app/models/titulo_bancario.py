from sqlalchemy import Column, Integer, String
from app.db import Base


class TituloBancario(Base):
    __tablename__ = 'titulos_bancarios'

    id = Column(Integer, primary_key=True, index=True)
    descricao = Column(String, index=True)
    codigo = Column(String, index=True)
