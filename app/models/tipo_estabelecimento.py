from sqlalchemy import Column, Integer, String
from app.db import Base


class TipoEstabelecimento(Base):
    __tablename__ = 'estabelecimentos_tipos'

    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String, index=True)
    descricao = Column(String, index=True)
