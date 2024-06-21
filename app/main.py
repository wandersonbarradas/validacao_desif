import time
from validator import ValidacaoDesif
from helpers.esquemas_e_leiautes import esquemas, leiautes

caminho = "C:/Users/wande/Downloads/pgcc_sa.txt"
inicio = time.time()
validation = ValidacaoDesif(caminho, esquemas, leiautes)
validation.validar()
print("\n".join(map(str, validation.erros)))
fim = time.time()
print(fim - inicio)
