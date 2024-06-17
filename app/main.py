import time
import polars as pl
from app.db import SessionLocal
from app.models.user import User
from app.models.cosif_conta import CosifConta


def get_user(user_id: int):
    db = SessionLocal()
    _user = db.query(User).filter(User.id == user_id).first()
    db.close()
    return _user


def get_cosif_conta(conta: str):
    db = SessionLocal()
    cosif_conta = db.query(CosifConta).filter(CosifConta.conta == conta).first()
    db.close()
    return cosif_conta


def get_all_cosif_contas():
    db = SessionLocal()
    cosif_contas = db.query(CosifConta).all()
    db.close()
    return cosif_contas


# Pegando contas do banco e gerando df
time_start = time.time()
contas = get_all_cosif_contas()
df_contas = pl.DataFrame({
    "conta": list(map(lambda x: x.conta, contas)),
    "conta_pontuacao": list(map(lambda x: x.conta_pontuacao, contas)),
    "conta_superior": list(map(lambda x: x.conta_superior, contas)),
    "nome": list(map(lambda x: x.nome, contas)),
    "funcao": list(map(lambda x: x.funcao, contas)),
    "grupo": list(map(lambda x: x.grupo, contas)),
    "subgrupo": list(map(lambda x: x.subgrupo, contas)),
    "desdobramento_subgrupo": list(map(lambda x: x.desdobramento_subgrupo, contas)),
    "titulo": list(map(lambda x: x.titulo, contas)),
    "subtitulo": list(map(lambda x: x.subtitulo, contas)),
    "digito_verificador": list(map(lambda x: x.digito_verificador, contas)),
})
time_end = time.time()
print("Tempo de execução para gerar o df a partir do banco de dados:", time_end - time_start)

# Pegando conta com filtro diretamente do banco
time_start = time.time()
conta = get_cosif_conta("73999007")
time_end = time.time()
print("Tempo de execução para pegar conta diretamente do banco de dados:", time_end - time_start)

# Pegando conta com filtro do df
time_start = time.time()
conta_df = df_contas.filter(pl.col('conta') == "73999007")
time_end = time.time()
print("Tempo de execução para pegar conta do df:", time_end - time_start)


