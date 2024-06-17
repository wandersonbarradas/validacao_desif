import time
from validator import Validador
from helpers.esquemas_e_leiautes import esquemas, leiautes


caminho = "C:/Users/DEV LENOVO/Desktop/pgcc.txt"
inicio = time.time()
validation = Validador(caminho, esquemas, leiautes)
validation.validar()
print("\n".join(map(str, validation.erros)))
fim = time.time()
print(fim - inicio)
