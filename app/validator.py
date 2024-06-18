import os
from helpers.helpers import criar_df, search_db
from models.titulo_bancario import TituloBancario
from models.municipio import Municipio
from models.codigo_tributacao import CodigoTributacao
from models.cosif_conta import CosifConta
import re
import polars as pl
from polars import Series
from typing import List, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta


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
        if self.arquivo_vazio():
            self.erros.append(self.gerar_mensagem_erro("EG018", ""))
            return
        if not self.validar_utf8():
            return
        self.modulo = self.df.row(0)[8]
        self.lista_registros = self.definir_registros()
        self.quebrar_dataframe_em_registros()
        self.verificar_registros_invalidos()  # EG012 (Todos)
        self.verificar_numero_de_colunas()  # EG014 (Todos)
        self.verificar_linhas()  # EG003, EG013 (Todos)
        if self.modulo in [1, 2, 3]:
            self.verificar_reg0000()  # ED035 (0000)
            self.verificar_versao_desif('3.1')  # ED043 (0000)
            self.verificar_registro0000_duplicado()  # ED037 (0000)
        self.verifica_registros_informados_incorretamente()  # EI030 (0100 0200 0300)
        self.verificar_contas_mistas_repetidas(self.blocos_registros['0100'])
        self.verificar_conta_cosif_e_superior_em_contas_mistas(self.blocos_registros['0100'])
        self.verificar_contas_de_receitas()
        self.verificar_campos()  # ED006(0000), EG008(Todos), EG007(0000, 0410) EG009(0000), ED015(0000), EG046 (Todos)

    def definir_registros(self):
        if self.modulo == '3':
            data = {}
            for i in ['0000', '0100']:
                data[i] = {
                    "esquema": self.esquemas[i],
                    "colunas": len(self.esquemas[i].keys())
                }
            return data

    def arquivo_vazio(self) -> bool:
        return os.path.getsize(self.caminho_arquivo) == 0

    def validar_utf8(self) -> bool:
        try:
            self.df = pl.scan_csv(self.caminho_arquivo, separator='|', encoding='utf8', has_header=False,
                                  schema_overrides=self.esquemas['padrao'], truncate_ragged_lines=True).collect()
            return True
        except UnicodeDecodeError:
            self.erros.append(self.gerar_mensagem_erro("EG019"))
        except Exception as e:
            self.erros.append("ERROR: Erro ao ler o arquivo: " + str(e))
        return False

    def verificar_reg0000(self) -> None:
        column = self.pegar_coluna('column_2')
        if column[0] != '0000':
            self.erros.append("ERROR: ED035 - Não foi informado o Registro 0000 ou "
                              "este não se encontra na primeira linha da declaração.")

    def verificar_versao_desif(self, version: str) -> None | bool:
        column = self.pegar_coluna('column_14')
        if not column:
            return False

        if column[0] != version:
            self.erros.append(self.gerar_mensagem_erro('ED043', {
                'Versão': column[0]
            }))

    def verificar_linhas(self) -> None:
        column = self.pegar_coluna('column_1')

        for i in range(1, len(column)):
            # Verifica se as linhas não são nulas

            if column[i - 1] is None:
                self.erros.append(self.gerar_mensagem_erro('EG013', {
                    'Ordem': i
                }))

            # Verifica se as linhas são sequenciais
            if column[i - 1] != str(i):
                self.erros.append(self.gerar_mensagem_erro('EG003', {
                    'Ordem': i
                }))

    def quebrar_dataframe_em_registros(self) -> None:
        blocks = {}
        if not self.lista_registros:
            return
        for registro in self.lista_registros.keys():
            if registro:
                block = self.df.filter(pl.col('column_2') == registro)
                if block.height > 0:
                    blocks[registro] = self.criar_dataframe(block, self.lista_registros[registro]['esquema'])

        self.blocos_registros = blocks

    def verificar_registros_invalidos(self) -> None:
        df_filtrado = self.df.filter(self.nao_numerico_ou_vazio(pl.col("column_2")))
        for i in range(0, df_filtrado.height):
            line = df_filtrado[i].select('column_1').row(0)[0]
            self.erros.append(self.gerar_mensagem_erro('EG012', {'Linha': line}))

    def verificar_numero_de_colunas(self) -> None:
        if not self.lista_registros:
            return
        with open(self.caminho_arquivo, 'r', encoding='utf-8') as file:
            for linha in file:
                content = linha.split('|')
                content.remove('\n')
                if len(content) > 0 and hasattr(self.lista_registros, content[1]):
                    if len(content) != self.lista_registros[content[1]]['colunas']:
                        self.erros.append(self.gerar_mensagem_erro('EG014', {
                            'Linha': content[0]
                        }))

    def verificar_registro0000_duplicado(self) -> None:
        data = self.df.filter(pl.col('column_2') == '0000')
        if data.height > 1:
            for i in range(1, data.height):
                self.erros.append(self.gerar_mensagem_erro('ED037', {
                    'Linha': data[i].select('column_1').row(0)[0]
                }))

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
                        if info_campo.get('type') == 'number':
                            self.verificar_campo_numerico(valor, campo, numero_linha)
                        self.verificar_tamanho_campos(valor, info_campo, campo, numero_linha)
                        if info_campo.get('required'):
                            self.verificacao_dinamica(valor, campo, numero_linha, lambda v: v is not None, 'EG046')
                        if registro == '0000':
                            self.verificar_campos_reg0000(linha, info_campo, campo, valor, numero_linha)
                        if registro == '0100':
                            self.verificar_campos_reg0100(linha, linha_anterior, linha_seguinte, info_campo, campo,
                                                          valor, numero_linha)

    def verificar_campos_reg0000(self, linha, info_campo, nome_campo, valor, numero_linha) -> None:
        if info_campo.get('type') == 'date':
            self.verificar_campo_data(valor, numero_linha, '0000')
        if nome_campo == 'tipo_inti':
            result = search_db(TituloBancario, 'codigo', valor.upper())
            self.verificacao_dinamica(result, nome_campo, numero_linha, lambda v: v, 'ED003')

        if nome_campo == 'cod_munc':
            self.verificacao_dinamica(valor, nome_campo, numero_linha, lambda v: v == '2203305',
                                      'ED059')
            result = search_db(Municipio, 'cod_ibge', valor)
            self.verificacao_dinamica(result, nome_campo, numero_linha, lambda v: v, 'EG001')

        if nome_campo == 'ano_mes_inic_cmpe':
            valor_ano_mes_fim_cmpe = linha.get_column('ano_mes_fim_cmpe')[0]
            self.verificar_ano_mes_inic_cmpe(valor, nome_campo, numero_linha, valor_ano_mes_fim_cmpe)
            pass
        if nome_campo == 'ano_mes_fim_cmpe':
            valor_ano_mes_inic_cmpe = linha.get_column('ano_mes_inic_cmpe')[0]
            self.verificar_ano_mes_fim_cmpe(valor, nome_campo, numero_linha, valor_ano_mes_inic_cmpe)
        if nome_campo == 'modu_decl':
            self.verificacao_dinamica(valor, nome_campo, numero_linha,
                                      lambda v: v in ['1', '2', '3', '4'], 'ED015')
        if nome_campo == 'tipo_decl':
            self.verificacao_dinamica(valor, nome_campo, numero_linha, lambda v: v in ['1', '2'],
                                      'ED006')
        if nome_campo == 'prtc_decl_ante':
            valor_tipo_decl = linha.get_column('tipo_decl')[0]
            self.verificar_protocolo(valor, nome_campo, numero_linha, valor_tipo_decl)
        if nome_campo == 'tipo_cnso':
            self.verificar_tipo_consolidacao(valor, nome_campo, numero_linha)
        if nome_campo == 'cnpj_resp_rclh':
            valor_tipo_cnso = linha.get_column('tipo_cnso')[0]
            valor_tipo_decl = linha.get_column('tipo_decl')[0]
            self.verificar_cnpj_responsavel(valor, nome_campo, numero_linha, valor_tipo_cnso,
                                            valor_tipo_decl),
        if nome_campo == 'tipo_arred':
            self.verificar_tipo_arredondamento(valor, nome_campo, numero_linha)

    def verificar_campos_reg0100(self, linha, linha_anterior, linha_seguinte, info_campo, nome_campo, valor,
                                 numero_linha) -> None:
        if nome_campo == 'des_mista':
            if int(valor) >= 2:
                des_anterior = linha_anterior.get_column('des_mista')[0]
                if des_anterior is not None:
                    self.verificacao_dinamica(int(des_anterior), nome_campo, numero_linha,
                                              lambda v: v == int(valor) - 1, 'EG030')
            elif int(valor) == 1:
                des_seguinte = linha_seguinte.get_column('des_mista')[0]
                if des_seguinte is not None:
                    self.verificacao_dinamica(int(des_seguinte), nome_campo, numero_linha, lambda v: v == 2, 'EG031')
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
        if nome_campo == 'conta_cosif':
            self.verificacao_dinamica(valor, nome_campo, numero_linha, lambda v: v[0] in ['7', '8'],
                                      'EG037')
            result = self.pegar_contas_cosif().filter(pl.col('conta') == valor)
            # self.verificacao_dinamica(result.height, nome_campo, numero_linha, lambda v: v > 0, 'EG036')
            pattern = r'(\d{1})(\d{1})(\d{1})(\d{2})(\d{2})(\d+)'
            resultado = re.search(pattern, valor)
            if resultado.group(4) == '00':
                cosif_repetidas = self.blocos_registros['0100'].filter(pl.col('conta_cosif') == valor)
                self.verificacao_dinamica(cosif_repetidas.height, nome_campo, numero_linha, lambda v: v == 1,
                                          'EG043')
        if nome_campo == 'conta_supe':
            valor_conta = linha.get_column('conta_cosif')[0]
            self.verificacao_dinamica(valor, nome_campo, numero_linha, lambda v: v != valor_conta, 'EG034')
            if valor is None:
                grupo = linha.get_column('conta_cosif')[0][0]
                primeira_linha = \
                    self.blocos_registros['0100'].filter(pl.col('conta_cosif').str.starts_with(grupo)).row(0)[0]
                self.verificacao_dinamica(numero_linha, nome_campo, numero_linha, lambda v: v == primeira_linha,
                                          'EG042')
            else:
                itens = self.blocos_registros['0100'].filter(pl.col('conta') == valor)
                if itens.height > 1:
                    for i in range(0, len(itens)):
                        _linha = itens[i].row(0)[0]
                        self.verificacao_dinamica(numero_linha, nome_campo, numero_linha, lambda v: v > _linha, 'EG049')
                # self.verificacao_dinamica(itens.height, nome_campo, numero_linha, lambda v: v > 0, 'EG033')
        if nome_campo == 'cod_trib_des_if':
            if valor is not None:
                result = search_db(CodigoTributacao, 'codigo', valor)
                self.verificacao_dinamica(result, nome_campo, numero_linha, lambda v: v, 'EG011')
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

    def verificar_tamanho_campos(self, valor, info_campo, nome_campo, numero_linha) -> None | bool:
        tamanho = info_campo.get('length')
        tamanho_exato = info_campo.get('exact_length')
        if valor is None:
            return False
        length = len(str(valor))
        if tamanho_exato:
            if length != tamanho:
                self.erros.append(self.gerar_mensagem_erro('EG009', {
                    'Linha': numero_linha,
                    'Campo': nome_campo,
                    'Tamanho': length
                }))
        else:
            if length > tamanho:
                self.erros.append(self.gerar_mensagem_erro('EG009', {
                    'Linha': numero_linha,
                    'Campo': nome_campo,
                    'Tamanho': length
                }))

    def verificar_campo_numerico(self, valor, nome_campo, numero_linha) -> None:
        if valor is not None:
            if self.numerico(f'{valor}') is False:
                self.erros.append(self.gerar_mensagem_erro('EG008', {
                    'Linha': numero_linha,
                    'Campo': nome_campo,
                    'Valor': valor
                }))

    def verificar_campo_data(self, valor, numero_linha, reg) -> None:
        if valor is not None:
            regex = r'^\d{4}(0[1-9]|1[0-2])$'
            if not bool(re.match(regex, valor)):
                self.erros.append(self.gerar_mensagem_erro('EG007', {
                    'Linha': numero_linha,
                    'Reg': reg,
                    'Data': valor,
                }))

    def verificar_tipo_consolidacao(self, valor, coluna, numero_linha):
        if str(self.modulo) == '2':
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is not None, 'ED012')
            self.verificacao_dinamica(valor, coluna, numero_linha,
                                      lambda v: v in ['1', '2', '3', '4'],
                                      'ED031')
        else:
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is None, 'ED021')

    def verificar_tipo_arredondamento(self, valor, coluna, numero_linha):
        if str(self.modulo) == '2':
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is not None,
                                      'ED044')
            self.verificacao_dinamica(valor, coluna, numero_linha,
                                      lambda v: v in ['1', '2'],
                                      'ED045')
        else:
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is None,
                                      'ED049')

    def verificar_cnpj_responsavel(self, valor, coluna, numero_linha, valor_tipo_cnso, valor_tipo_decl):
        if valor_tipo_cnso in ['1', '2']:
            self.verificacao_dinamica(valor, coluna, numero_linha,
                                      lambda v: v is not None, 'ED013')
        elif valor_tipo_cnso in ['3', '4']:
            self.verificacao_dinamica(valor, coluna, numero_linha,
                                      lambda v: v is None, 'ED051')
        if valor_tipo_decl != '2':
            self.verificacao_dinamica(valor, coluna, numero_linha,
                                      lambda v: v is None, 'ED048')

    def verificar_protocolo(self, valor, coluna, numero_linha, tipo_declaracao):
        if tipo_declaracao == '2':
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is not None,
                                      'ED024')
        else:
            self.verificacao_dinamica(valor, coluna, numero_linha, lambda v: v is None,
                                      'ED026')

    def verificar_ano_mes_inic_cmpe(self, valor, nome_campo, numero_linha, valor_ano_mes_fim_cmpe) -> None:
        if valor is not None:
            data_atual = self.criar_data((datetime.now()).strftime("%Y%m")).date()
            data_inicio_competencia = self.criar_data(valor).date()
            self.verificacao_dinamica(data_inicio_competencia, nome_campo, numero_linha, lambda v: v < data_atual,
                                      'ED005')
            if str(self.modulo) == '2':
                data_fim_competencia = self.criar_data(valor_ano_mes_fim_cmpe).date()
                self.verificacao_dinamica(data_fim_competencia, nome_campo, numero_linha,
                                          lambda v: v == data_inicio_competencia, 'ED023')

    def verificar_ano_mes_fim_cmpe(self, valor, nome_campo, numero_linha, valor_ano_mes_inic_cmpe) -> None:
        if valor is not None:
            data_limite = self.criar_data((datetime.now() - relativedelta(years=10)).strftime("%Y%m")).date()
            data_competencia = self.criar_data(valor).date()
            data_inicio_competencia = self.criar_data(valor_ano_mes_inic_cmpe).date()
            self.verificacao_dinamica(data_competencia, nome_campo, numero_linha,
                                      lambda v: v >= data_limite, 'ED004')
            self.verificacao_dinamica(data_competencia, nome_campo, numero_linha,
                                      lambda v: v >= data_inicio_competencia, 'ED054')
            if str(self.modulo) in ['3', '4']:
                self.verificacao_dinamica(data_competencia.year, nome_campo, numero_linha,
                                          lambda v: v == data_competencia.year, 'ED052')

    def verificacao_dinamica(self, valor, nome_campo: str, numero_linha, validation_fn, error_code: str) -> None:
        if not validation_fn(valor):
            self.erros.append(self.gerar_mensagem_erro(error_code, {
                'Linha': numero_linha,
                'Campo': nome_campo
            }))

    def pegar_coluna(self, column_name: str) -> list | bool:
        columns = self.df.columns
        if column_name in columns:
            return self.df.get_column(column_name).to_list()
        return False

    def verificar_contas_mistas_repetidas(self, df: pl.DataFrame):
        df_duplicados = df.groupby("conta", "des_mista").agg(pl.count("conta").alias("contagem"))
        df_duplicados = df_duplicados.filter(pl.col("contagem") > 1)
        df_linhas_repetidas = df.join(df_duplicados, on="conta", how="inner")
        if df_linhas_repetidas.height > 1:
            for i in range(0, df_linhas_repetidas.height):
                pass
                # self.erros.append(
                # self.gerar_mensagem_erro('EI001', {'Linha': df_linhas_repetidas.row(i)[0], 'Campo': 'Conta'}))

    def verificar_conta_cosif_e_superior_em_contas_mistas(self, df: pl.DataFrame):
        df_duplicados = df.groupby("conta").agg(pl.count("conta").alias("contagem"))
        df_duplicados = df_duplicados.filter(pl.col("contagem") > 1)
        df_linhas_repetidas = df.join(df_duplicados, on="conta", how="inner")
        for i in range(0, df_duplicados.height):
            linhas = df_linhas_repetidas.filter(pl.col("conta") == df_duplicados.row(i)[0])
            conta_mista = linhas.filter(pl.col("des_mista") == '00')
            if conta_mista.height > 0:
                conta_cosif = conta_mista.row(0)[7]
                conta_superior = conta_mista.row(0)[6]
                for j in range(0, linhas.height):
                    self.verificacao_dinamica(linhas.row(j)[7], "conta_cosif",
                                              linhas.row(j)[0], lambda v: v == conta_cosif, 'EG044')
                    self.verificacao_dinamica(linhas.row(j)[6], "conta_superior",
                                              linhas.row(j)[0], lambda v: v == conta_superior, 'EG050')
            else:
                self.erros.append(
                    self.gerar_mensagem_erro('EG032', {'Linha': linhas.row(0)[0], 'Campo': 'des_mista'}))

    def verificar_contas_de_receitas(self):
        contas = self.blocos_registros['0100'].filter(pl.col('conta_cosif').str.starts_with('7'))
        if contas.height == 0:
            self.erros.append(self.gerar_mensagem_erro('EI023', {'Linha': '1', 'Modulo': self.modulo}))

    def pegar_contas_cosif(self):
        if self.contas_cosif is None:
            self.contas_cosif = criar_df(CosifConta)
        return self.contas_cosif

    @staticmethod
    def gerar_mensagem_erro(cod: str, fields=None) -> str:
        error_message = {
            "EG019": "Arquivo não está codificado em UTF-8 ou existem caracteres inválidos no arquivo.",
            "EG018": "Arquivo vazio.",
            "ED035": "Não foi informado o Registro 0000 ou este não se encontra na primeira linha da declaração.",
            "ED043": "Indicador de versão da DES-IF inexistente ou não aceito por este aplicativo.",
            "EG013": "Existe ocorrência sem número da linha.",
            "EG003": "Número da linha no arquivo TXT fora de sequência.",
            "EG012": "Tipo de registro inválido ou não informado.",
            "EG014": "O número de colunas está diferente do definido no leiaute para o Registro.",
            "ED037": "Registro 0000 em duplicidade. O Registro 0000 deve ser único.",
            "EC021": "Registro informado indevidamente. Este tipo de registro não compõe o módulo da declaração que "
                     "está sendo entregue.",
            "ED063": "Registro informado indevidamente. Este tipo de registro não compõe o módulo da declaração que "
                     "está sendo entregue.",
            "EI030": "Registro informado indevidamente. Este tipo de registro não compõe o módulo da declaração que "
                     "está sendo entregue.",
            "EL007": "Registro informado indevidamente. Este tipo de registro não compõe o módulo da declaração que "
                     "está sendo entregue.",
            "EM095": "Registro informado indevidamente. Este tipo de registro não compõe o módulo da declaração que "
                     "está sendo entregue.",
            "EG008": "Campo preenchido com valor inválido. Campos numéricos só podem apresentar algarismos de 0 a 9 "
                     "e, no caso de valor, utilizar a vírgula como separador de decimais e o traço ‘-‘, "
                     "quando negativo.",
            "EG009": "Tamanho do campo diferente do especificado no leiaute.",
            "EG007": "Ano-mês inválido. Deve ser informada uma competência válida no formato aaaamm.",
            "EG037": "Conta COSIF não pertence aos Grupos 7 ou 8.",
            "EG046": "O campo foi deixado em branco. É obrigatório o preenchimento deste campo.",
            "ED006": "Tipo de declaração informado no Registro 0000 está errado. Só pode ser‘1’ (Normal) ou ‘2’ ("
                     "Retificadora).",
            "ED015": "O módulo da declaração informado no Registro 0000 está errado. Deve ser ‘1’ para Demonstrativo "
                     "Contábil ou ‘2’ para Apuração Mensal do ISSQN ou ‘3’ para Informações Comuns aos Municípios ou "
                     "‘4’ para Partidas dos Lançamentos Contábeis.",
            "ED021": "Tipo de consolidação informado indevidamente no Registro 0000. Só deve ser informado se o "
                     "módulo da declaração for ‘2’ (Apuração Mensal do ISSQN).",
            "ED031": "O tipo de consolidação informado no Registro 0000 está errado. Só pode ser ‘1’, ‘2’, ‘3’ ou ‘4’.",
            "ED045": "O tipo de arredondamento informado no Registro 0000 está errado. Só pode ser ‘1’ ou ‘2’.",
            "ED044": "O tipo de arredondamento não foi informado no Registro 0000. Deve ser informado quando o módulo "
                     "da declaração for ‘2’ (Apuração Mensal do ISSQN).",
            "ED012": "O tipo de consolidação não foi informado no Registro 0000. Deve ser informado sempre que o "
                     "módulo da declaração for ‘2’ (Apuração Mensal do ISSQN).",
            "ED013": "Não foi informado o CNPJ responsável pelo recolhimento no Registro 0000. É obrigatório se o "
                     "tipo de consolidação informado no Registro 0000 for ‘1’ ou ‘2’.",
            "ED024": "Não foi informado o protocolo da declaração anterior. Se o tipo da declaração no Registro 0000 "
                     "for ‘2’ (retificadora), essa informação é obrigatória.",
            "ED026": "Código do protocolo informado indevidamente no Registro 0000.Só pode ser informado se o tipo de "
                     "declaração for ‘2’ (retificadora).",
            "ED048": "O responsável pelo recolhimento foi informado no Registro 0000. Não deve ser informado quando o "
                     "módulo da declaração for diferente de ‘2’.",
            "ED049": "O tipo de arredondamento foi informado no Registro 0000. Não deve ser informado quando o módulo "
                     "da declaração for diferente de ‘2’.",
            "ED051": "O responsável pelo recolhimento foi informado no Registro 0000. Não deve ser informado quando o "
                     "tipo de consolidação foi igual a ‘3’ (dependência e alíquota) ou ‘4’ (dependência, alíquota e "
                     "código de tributação DES-IF).",
            "ED004": "O ano de competência informado no Registro 0000 é anterior a 10 anos. Entregar somente "
                     "declarações de no máximo 10 anos atrás.",
            "ED005": "Competência inicial (ano-mês) é maior ou igual à data atual.",
            "ED023": "Ano-mês de inicio da competência é diferente do ano-mês do fim da competência. No campo "
                     "“Modu_Decl” do Registro 0000 foi informado ‘2’ (Apuração Mensal do ISSQN), portanto as "
                     "competências de início e fim devem ser iguais.",
            "ED052": "O ano de competência final informado no Registro 0000 difere do ano de competência inicial. No "
                     "módulo da declaração do Registro 0000 foi informado ‘3’ (PGCC) ou ‘4’ (Partida de Lançamentos "
                     "Contábeis), portanto as competências de início e fim devem estar no mesmo exercício fiscal.",
            "ED054": "Data de fim da competência é anterior à data de início da competência.",
            "ED059": "O Código do Município está incorreto. Não condiz com o Município para o qual está sendo "
                     "prestada a declaração.",
            "EG034": "Conta Superior não pode ser igual à própria Conta.",
            "EG042": "Conta superior não foi informada. É obrigatório quando não for Grupo Inicial do COSIF.",
            "EI001": "O conjunto conta e desdobramento foi informado mais de uma vez no PGCC.",
            "ED003": "O tipo da instituição informado no registro 0000 não existe na Tabela de Títulos (Anexo 2).",
            "EG001": "O código do município informado não existe na Tabela de Municípios do IBGE (Anexo 5).",
            "EG011": "O código de tributação DES-IF informado não existe na Tabela de Códigos de Tributação DES-IF ("
                     "Anexo 6).",
            "EG030": "Foi informado desdobramento sem observar a sequência numérica. O desdobramento informado nesta "
                     "linha é maior ou igual a ‘02’, porém não existe a numeração imediatamente anterior.",
            "EG031": "Foi informado apenas o desdobramento de conta mista ‘01’, sem informar outro desdobramento.",
            "EG049": "Conta superior (campo 7 deste registro) aparece como conta de nível hierárquico inferior",
            "EG033": "Conta aparece como superior e não foi definida no Plano de Contas ou no Balancete Analítico "
                     "para a dependência no mês.",
            "EG036": "Conta COSIF inexistente na Tabela do COSIF.",
            "EG044": "A conta COSIF informada na conta desdobrada é diferente da conta COSIF informada na conta "
                     "mista. Se foi informado desdobramento de conta mista, tanto as contas desdobradas (filhas) "
                     "quanto a conta mista (mãe) devem ter o mesmo COSIF analítico.",
            "EG050": "A conta superior informada na conta desdobrada é diferente da conta superior informada na conta "
                     "mista. Se foi informado desdobramento de conta mista, tanto as contas desdobradas (filhas) "
                     "quanto a conta mista (mãe) devem referenciar a mesma conta superior.",
            "EI023": "Não existem contas de receitas no Plano Geral de Contas Comentado - PGCC (R0100) do módulo "
                     "Informações Comuns aos Municípios.",
            "EI004": "Descrição da conta não foi informada. É obrigatório quando a conta for mais analítica, "
                     "bem como para os desdobramentos de conta mista.",
            "EI010": "Subtítulo é conta superior ou conta mista e possui código de tributação DES-IF. Somente as "
                     "contas mais analíticas podem possuir código de tributação.",
            "EG043": "Código COSIF em duplicidade. Uma conta COSIF de nível não analítico só pode corresponder a uma "
                     "única conta do PGCC ou Balancete Analítico.",
            "EG032": "Foi informado desdobramento de conta mista, porém não foi informada a conta mista (‘00’)."
        }
        message = error_message.get(cod, "Código de erro desconhecido.")

        if fields:
            formatted_fields = "; ".join([f"{key}: {value}" for key, value in fields.items()])
            message += f" {formatted_fields}."

        return f"{cod}: {message}"

    @staticmethod
    def criar_dataframe(data: pl.DataFrame, esquema) -> pl.DataFrame:
        columns = list(esquema.keys())
        new_df = pl.DataFrame({
            columns[i]: data[f'column_{i + 1}'].cast(esquema[columns[i]])
            for i in range(len(columns))
        }, strict=False)
        return new_df

    @staticmethod
    def nao_numerico_ou_vazio(s: pl.Series) -> Series:
        return (~s.str.contains(r'^\d+$')) | (s.is_null())

    @staticmethod
    def numerico(s: str) -> bool:
        if not s:
            return True

        # Expressão regular para validar números inteiros ou decimais com vírgula
        regex = r'^-?\d+(,\d+)?$'
        return bool(re.match(regex, s))

    @staticmethod
    def criar_data(data_str: str):
        try:
            if len(data_str) == 6:
                # Formato aaaamm
                data = datetime.strptime(data_str, "%Y%m")
            elif len(data_str) == 8:
                # Formato aaaammdd
                data = datetime.strptime(data_str, "%Y%m%d")
            else:
                raise ValueError("Formato de data inválido. Use aaaamm ou aaaammdd.")
            return data
        except ValueError as e:
            print(f"Erro ao converter a data: {e}")
            return None
