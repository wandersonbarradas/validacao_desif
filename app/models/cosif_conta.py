from sqlalchemy import Column, Integer, String
from app.db import Base


class CosifConta(Base):
    __tablename__ = 'cosif_contas'

    id = Column(Integer, primary_key=True, index=True)
    conta = Column(String, index=True, unique=True)
    conta_pontuacao = Column(String, index=True, unique=True)
    conta_superior = Column(String, index=True)
    nome = Column(String, index=True)
    funcao = Column(String, index=True)
    grupo = Column(String, index=True)
    subgrupo = Column(String, index=True)
    desdobramento_subgrupo = Column(String, index=True)
    titulo = Column(String, index=True)
    subtitulo = Column(String, index=True)
    digito_verificador = Column(String, index=True)
