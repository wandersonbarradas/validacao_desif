import time
from app.classes.validador_desif import ValidacaoDesif
from app.helpers.esquemas_e_leiautes import esquemas, leiautes

caminho = "app/pgcc.txt.p7s"


def validar():
    inicio = time.time()
    validation = ValidacaoDesif(caminho, esquemas, leiautes)
    validation.validar()
    print("\n".join(map(str, validation.erros)))
    fim = time.time()
    print(fim - inicio)
