from helpers.helpers import search_db
from models.codigo_tributacao import CodigoTributacao
import re
import polars as pl
from typing import List, Optional


class Validador:

    def __init__(self, caminho_arquivo: str, esquemas, leiautes):
        self.caminho_arquivo: str = caminho_arquivo
        self.esquemas = esquemas
        self.leiautes = leiautes
        self.df: Optional[pl.DataFrame] = None
        self.lista_registros = None
        self.modulo: Optional[int] = None
        self.blocos_registros: Optional[dict[str, pl.DataFrame]] = None
        self.erros: Optional[List[str]] = []
        self.contas_cosif: Optional[pl.DataFrame] = None

    def validar(self) -> None:
        self.modulo = self.df.row(0)[8]
        self.verifica_registros_informados_incorretamente()  # EI030 (0100 0200 0300)
        self.verificar_contas_mistas_repetidas(self.blocos_registros['0100'])
        self.verificar_contas_de_receitas()
        self.verificar_campos()  # ED006(0000), EG008(Todos), EG007(0000, 0410) EG009(0000), ED015(0000), EG046 (Todos)

    def verificar_campos(self) -> None:
        if not self.lista_registros:
            return
        for registro in self.lista_registros.keys():
            if registro in self.blocos_registros.keys():
                for i in range(0, len(self.blocos_registros[registro])):
                    linha = self.blocos_registros[registro][i]
                    linha_anterior = None
                    linha_seguinte = None
                    if i > 0:
                        linha_anterior = self.blocos_registros[registro][i - 1]
                    if i + 1 < len(self.blocos_registros[registro]):
                        linha_seguinte = self.blocos_registros[registro][i + 1]
                    numero_linha = linha.get_column('num_linha')[0]
                    colunas = linha.columns
                    info_campos = self.leiautes[registro]
                    for campo in colunas:
                        info_campo = info_campos.get(campo)
                        valor = linha.get_column(campo)[0]
                        if registro == '0000':
                            self.verificar_campos_reg0000(linha, info_campo, campo, valor, numero_linha)
                        if registro == '0100':
                            self.verificar_campos_reg0100(linha, linha_anterior, linha_seguinte, info_campo, campo,
                                                          valor, numero_linha)

    def verificar_campos_reg0000(self, linha, info_campo, nome_campo, valor, numero_linha) -> None:
        if nome_campo == 'cod_munc':
            self.verificacao_dinamica(valor, nome_campo, numero_linha, lambda v: v == '2203305',
                                      'ED059')


    def verificar_campos_reg0100(self, linha, linha_anterior, linha_seguinte, info_campo, nome_campo, valor,
                                 numero_linha) -> None:
        if nome_campo == 'conta':
            if valor is not None:
                if valor[5:7] == '00':
                    contas_inferiores = self.blocos_registros['0100'].filter(pl.col('conta_supe') == valor)
                    self.verificacao_dinamica(contas_inferiores.height, nome_campo, numero_linha, lambda v: v > 0,
                                              'EI028')
        if nome_campo == 'des_mista':
            if valor != "00":
                conta = linha.get_column('conta')[0]
                if conta[5:7] != '00':
                    linhas = self.blocos_registros['0100'].filter(
                        (pl.col('conta').str.starts_with(conta[:-1])) & (pl.col('conta') != conta))
                    if linhas.height > 0:
                        self.erros.append(self.gerar_mensagem_erro('EG051', {
                            'Linha': numero_linha,
                            'Campo': nome_campo
                        }))
                else:
                    self.erros.append(self.gerar_mensagem_erro('EG051', {
                        'Linha': numero_linha,
                        'Campo': nome_campo
                    }))

        if nome_campo == 'desc_conta':
            if valor is None:
                conta = linha.get_column('conta')[0]
                pattern = r'(\d{1})(\d{1})(\d{1})(\d{2})(\d{2})(\d+)'
                resultado = re.search(pattern, conta)
                if resultado.group(5) != '00':
                    self.erros.append(self.gerar_mensagem_erro('EI004', {
                        'Linha': numero_linha,
                        'Campo': nome_campo
                    }))
                elif resultado.group(4) != '00':
                    self.erros.append(self.gerar_mensagem_erro('EI004', {
                        'Linha': numero_linha,
                        'Campo': nome_campo
                    }))

        if nome_campo == 'cod_trib_des_if':
            if valor is not None:
                conta = linha.get_column('conta')[0]
                desdobro = linha.get_column('des_mista')[0]
                contas_filhas = self.blocos_registros['0100'].filter(pl.col('conta_supe') == conta)
                if contas_filhas.height > 0 or desdobro == '00':
                    self.erros.append(self.gerar_mensagem_erro('EI010', {'Linha': numero_linha, 'Campo': nome_campo}))

    def verifica_registros_informados_incorretamente(self) -> None:
        if self.modulo == '3':
            data = self.df.filter(~pl.col('column_2').is_in(self.lista_registros.keys()))
            if data.height > 0:
                for i in range(0, data.height):
                    line = data[i].select('column_1').row(0)[0]
                    reg = data[i].select('column_2').row(0)[0]
                    self.erros.append(self.gerar_mensagem_erro('EI030', {'Linha': line, 'Registro': reg}))

    def verificar_contas_mistas_repetidas(self, df: pl.DataFrame):
        df_duplicados = df.groupby("conta", "des_mista").agg(pl.count("conta").alias("contagem"))
        df_duplicados = df_duplicados.filter(pl.col("contagem") > 1)
        df_linhas_repetidas = df.join(df_duplicados, on="conta", how="inner")
        if df_linhas_repetidas.height > 1:
            for i in range(0, df_linhas_repetidas.height):
                pass
                # self.erros.append(
                # self.gerar_mensagem_erro('EI001', {'Linha': df_linhas_repetidas.row(i)[0], 'Campo': 'Conta'}))

    def verificar_contas_de_receitas(self):
        contas = self.blocos_registros['0100'].filter(pl.col('conta_cosif').str.starts_with('7'))
        if contas.height == 0:
            self.erros.append(self.gerar_mensagem_erro('EI023', {'Linha': '1', 'Modulo': self.modulo}))
