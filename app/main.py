import time
from app.classes.validador_desif import ValidacaoDesif
from app.helpers.esquemas_e_leiautes import esquemas, leiautes

caminho = "C:/Users/DEV LENOVO/Desktop/DESIF/validacao_desif/pgcc.txt.p7s"


def validar():
    inicio = time.time()
    validation = ValidacaoDesif(caminho, esquemas, leiautes)
    validation.validar()
    # print("############ LISTA DE ERROS: ############")
    # print("\n".join(map(str, validation.erros)))
    print("TOTAL DE ERROS: " + str(len(validation.erros)))
    fim = time.time()
    print(fim - inicio)
