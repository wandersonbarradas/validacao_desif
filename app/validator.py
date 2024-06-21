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
from helpers.esquemas_e_leiautes import esquemas, leiautes


class IdentificacaoDeclaracao:
    def __init__(self, reg0000: pl.DataFrame, modulo):
        self.reg0000: pl.DataFrame = reg0000
        self.modulo = str(modulo)
        self.erros = []

    def validar(self):
        self.ed004_ed005_ed023_ed052_ed054_eg007()
        self.ed012_ed031()
        self.ed013_ed021_ed048_ed051()
        self.ed015()
        self.ed037()
        self.loop_campos()
        return self.erros

    def ed003(self, valor, num_linha) -> None:
        if valor is not None:
            result = search_db(TituloBancario, 'codigo', valor.upper())
            if result is not None:
                return
        self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED003'})

    def ed004_ed005_ed023_ed052_ed054_eg007(self) -> None:
        linha = self.reg0000.get_column('num_linha')[0]
        valor = self.reg0000.get_column('ano_mes_fim_cmpe')[0]
        valor_ano_mes_inic_cmpe = self.reg0000.get_column('ano_mes_inic_cmpe')[0]
        if valor is not None:
            if ValidacaoDesif.validar_data(valor_ano_mes_inic_cmpe) is False:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'EG007'})
                return
            if ValidacaoDesif.validar_data(valor) is False:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'EG007'})
                return
            data_limite = ValidacaoDesif.criar_data((datetime.now() - relativedelta(years=10)).strftime("%Y%m")).date()
            data_atual = ValidacaoDesif.criar_data((datetime.now()).strftime("%Y%m")).date()
            data_fim_competencia = ValidacaoDesif.criar_data(valor).date()
            data_inicio_competencia = ValidacaoDesif.criar_data(valor_ano_mes_inic_cmpe).date()
            if data_fim_competencia < data_limite:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED004'})
            if data_inicio_competencia >= data_atual:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED005'})
            if self.modulo == '2' and data_fim_competencia != data_inicio_competencia:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED023'})
            if data_fim_competencia < data_inicio_competencia:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED054'})
            if self.modulo in ['3', '4'] and data_inicio_competencia.year != data_fim_competencia.year:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED052'})

    def ed006(self, valor, num_linha) -> None:
        if valor not in ['1', '2']:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED006'})

    def ed012_ed031(self):
        linha = self.reg0000.get_column('num_linha')[0]
        valor = self.reg0000.get_column('tipo_decl')[0]
        if str(self.modulo) == '2':
            if valor is None:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED012'})
            if valor not in ['1', '2', '3', '4']:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED031'})

    def ed013_ed021_ed048_ed051(self):
        linha = self.reg0000.get_column('num_linha')[0]
        valor = self.reg0000.get_column('cnpj_resp_rclh')[0]
        valor_tipo_cnso = self.reg0000.get_column('tipo_cnso')[0]
        if valor_tipo_cnso in ['1', '2'] and valor is None:
            self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED013'})
        elif valor_tipo_cnso in ['3', '4'] and valor is not None:
            self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED051'})
        if self.modulo != '2' and valor is not None:
            self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED048'})
        elif self.modulo != '2' and valor_tipo_cnso is not None:
            self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED021'})
        # elif self.modulo == '2' and valor is None:
        #     self.errors.append({"Linha": linha, "Reg": '0000', "Erro": 'ED048'})

    def ed015(self):
        linha = self.reg0000.get_column('num_linha')[0]
        if self.modulo not in ['1', '2', '3', '4']:
            self.erros.append({"linha": linha, "Reg": '0000', "erro": 'ED015'})

    def ed024_ed026(self, valor, num_linha) -> None:
        if self.modulo == '2' and valor is None:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED024'})
        elif self.modulo != '2' and valor is not None:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED026'})

    def ed037(self) -> None:
        if self.reg0000.height > 1:
            for i in range(0, self.reg0000.height):
                linha = self.reg0000.get_column('num_linha')[i]
                self.erros.append({"Linha": linha, "Reg": '0000', "Erro": 'ED037'})

    def ed043(self, valor, num_linha) -> None:
        if valor is None or valor != '3.1':
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED043'})

    def ed044_ed045_ed049(self, valor, num_linha):
        if self.modulo == '2' and valor is None:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED044'})
        elif self.modulo == '2' and valor not in ['1', '2']:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED045'})
        elif self.modulo != '2' and valor is not None:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'ED049'})

    def eg001(self, valor, num_linha) -> None:
        resultado = ValidacaoDesif.validar_municipio(valor)
        if not resultado:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'EG001'})

    def loop_campos(self):
        linha = self.reg0000[0]
        numero_linha = linha.get_column('num_linha')[0]
        nome_colunas = linha.columns
        info_campos = leiautes['0000']
        for nome_campo in nome_colunas:
            info_campo = info_campos.get(nome_campo)
            valor = linha.get_column(nome_campo)[0]
            if info_campo.get('type') == 'number' and valor is not None and not ValidacaoDesif.validar_numerico(
                    str(valor)):
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG008'})
            if not ValidacaoDesif.validar_tamanho(valor, info_campo):
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG009'})
            if info_campo.get('required') and valor is None:
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG046'})
            if nome_campo == 'cod_munc':
                self.eg001(valor, numero_linha)
            if nome_campo == 'tipo_inti':
                self.ed003(valor, numero_linha)
            if nome_campo == 'tipo_decl':
                self.ed006(valor, numero_linha)
            if nome_campo == 'prtc_decl_ante':
                self.ed024_ed026(valor, numero_linha)
            if nome_campo == 'idn_versao':
                self.ed043(valor, numero_linha)
            if nome_campo == 'tipo_arred':
                self.ed044_ed045_ed049(valor, numero_linha)


class PlanoGeralContasComentado:

    def __init__(self, reg0100: pl.DataFrame, df_cosifs: pl.DataFrame):
        self.reg0100: pl.DataFrame = reg0100
        self.df_cosifs: pl.DataFrame = df_cosifs
        self.erros = []

    def validar(self):
        self.loop_campos()
        self.eg032_eg044_eg050()
        return self.erros

    def loop_campos(self):
        for i in range(0, self.reg0100.height):
            linha = self.reg0100[i]
            numero_linha = linha.get_column('num_linha')[0]
            nome_colunas = linha.columns
            info_campos = leiautes['0100']
            for nome_campo in nome_colunas:
                info_campo = info_campos.get(nome_campo)
                valor = linha.get_column(nome_campo)[0]
                # Validações padrões
                if info_campo.get('type') == 'number' and valor is not None and not ValidacaoDesif.validar_numerico(
                        str(valor)):
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG008'})
                if not ValidacaoDesif.validar_tamanho(valor, info_campo):
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG009'})
                if info_campo.get('required') and valor is None:
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG046'})
                # Validações dos campos
                if nome_campo == 'des_mista':
                    if valor is not None and valor.isdigit():
                        if int(valor) >= 2 and i > 0:
                            linha_anterior = self.reg0100[i - 1]
                            if not ValidacaoDesif.eg030(valor, linha_anterior):
                                self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG030'})
                        elif int(valor) == 1:
                            linha_seguinte = self.reg0100[i + 1]
                            if not ValidacaoDesif.eg031(linha_seguinte):
                                self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG031'})
                if nome_campo == 'conta_supe':
                    self.eg033_eg034_eg042_eg049(valor, linha, numero_linha)
                if nome_campo == 'conta_cosif':
                    self.eg036_eg037_eg039_eg043(valor, linha, numero_linha)
                if nome_campo == 'cod_trib_des_if':
                    if valor is not None:
                        self.eg011(valor, numero_linha)

    def eg011(self, valor, num_linha):
        result = ValidacaoDesif.validar_cod_tributacao(valor)
        if not result:
            self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EG011'})

    def eg032_eg044_eg050(self):
        erros = ValidacaoDesif.validar_conta_cosif_e_superior_em_contas_mistas(self.reg0100, '0100')
        if len(erros) > 0:
            self.erros.extend(erros)

    def eg033_eg034_eg042_eg049(self, valor, linha, numero_linha):
        erros = ValidacaoDesif.validar_conta_superior(valor, linha, numero_linha, self.reg0100, '0100')
        if len(erros) > 0:
            self.erros.extend(erros)

    def eg036_eg037_eg039_eg043(self, valor, linha: pl.DataFrame, num_linha):
        errors = ValidacaoDesif.validar_conta_cosif(valor, linha, self.reg0100, self.df_cosifs, num_linha, '0100')
        if len(errors) > 0:
            self.erros.extend(errors)


class ValidacaoDesif:

    def __init__(self, caminho_arquivo: str, esquemas, leiautes):
        self.caminho_arquivo: str = caminho_arquivo
        self.esquemas = esquemas
        self.leiautes = leiautes
        self.df: Optional[pl.DataFrame] = None
        self.lista_registros = None
        self.modulo: Optional[int] = None
        self.blocos_registros: Optional[dict[str, pl.DataFrame]] = None
        self.erros = []
        self.contas_cosif: Optional[pl.DataFrame] = None

    def validar(self) -> None:
        if not self.eg018():
            self.erros.append({"erro": "EG018"})
            return
        if not self.eg019():
            return
        self.modulo = self.df.row(0)[8]
        self.lista_registros = self.definir_registros()
        self.quebrar_dataframe_em_registros()
        self.pegar_contas_cosif()
        self.eg003_eg013()
        self.eg012()
        self.eg014()
        if self.ed035():
            ident_declaracao = IdentificacaoDeclaracao(self.blocos_registros['0000'], self.modulo)
            erros = ident_declaracao.validar()
            self.erros.extend(erros)
        if '0100' in self.lista_registros:
            pgcc = PlanoGeralContasComentado(self.blocos_registros['0100'], self.contas_cosif)
            erros = pgcc.validar()
            self.erros.extend(erros)

    def eg018(self) -> bool:
        try:
            return os.path.getsize(self.caminho_arquivo) > 0
        except Exception as e:
            print("ERROR (EG018): Erro ao ler o arquivo: " + str(e))
            quit()

    def eg019(self) -> bool:
        try:
            self.df = pl.scan_csv(self.caminho_arquivo, separator='|', encoding='utf8', has_header=False,
                                  schema_overrides=self.esquemas['padrao'], truncate_ragged_lines=True).collect()
            return True
        except UnicodeDecodeError:
            self.erros.append({"erro": "EG019"})
        except Exception as e:
            self.erros.append("ERROR (EG019): Erro ao ler o arquivo: " + str(e))
        return False

    def definir_registros(self):
        if self.modulo == '3':
            data = {}
            for i in ['0000', '0100']:
                data[i] = {
                    "esquema": self.esquemas[i],
                    "colunas": len(self.esquemas[i].keys())
                }
            return data

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

    def ed035(self) -> bool:
        reg = self.df.get_column('column_2')[0]
        if reg != '0000':
            self.erros.append({"erro": 'ED035'})
            return False
        return True

    def eg003_eg013(self) -> None:
        linhas = self.df.get_column('column_1')

        for i in range(0, len(linhas)):
            if linhas[i] is None:
                self.erros.append({"linha": i + 1, "erro": 'EG013'})
            if linhas[i] != str(i + 1):
                self.erros.append({"linha": i + 1, "erro": 'EG003'})

    def eg012(self) -> None:
        df_filtrado = self.df.filter(self.nao_numerico_ou_vazio(pl.col("column_2")))
        for i in range(0, df_filtrado.height):
            numero_linha = df_filtrado[i].select('column_1').row(0)[0]
            self.erros.append({'linha': numero_linha, "erro": 'EG012'})

    def eg014(self) -> None:
        if not self.lista_registros:
            return
        with open(self.caminho_arquivo, 'r', encoding='utf-8') as file:
            for linha in file:
                content = linha.split('|')
                content.remove('\n')
                if len(content) > 0 and content[1] in self.lista_registros:
                    if len(content) != self.lista_registros[content[1]]['colunas']:
                        self.erros.append({'linha': content[0], 'reg': content[1], "erro": 'EG014'})

    def pegar_contas_cosif(self):
        if self.contas_cosif is None:
            self.contas_cosif = criar_df(CosifConta)
        return self.contas_cosif

    @staticmethod
    def criar_dataframe(data: pl.DataFrame, esquema) -> pl.DataFrame:
        columns = list(esquema.keys())
        new_df = pl.DataFrame({
            columns[i]: data[f'column_{i + 1}'].cast(esquema[columns[i]])
            for i in range(len(columns))
        }, strict=False)
        return new_df

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

    @staticmethod
    def validar_municipio(valor: str):
        result = search_db(Municipio, 'cod_ibge', valor)
        if result:
            return True
        return False

    @staticmethod
    def validar_cod_tributacao(valor):
        result = search_db(CodigoTributacao, 'codigo', valor)
        if result:
            return True
        return False

    @staticmethod
    def validar_conta_cosif(valor, linha: pl.DataFrame, df: pl.DataFrame, df_cosif: pl.DataFrame, num_linha: int | str,
                            reg: str):
        # result = df_cosifs.filter(pl.col('conta') == valor)
        # if result.height > 0:
        #     return True
        # return False
        erros = []
        conta_superior = linha.get_column('conta_supe')[0]
        if conta_superior is not None:
            linhas_conta_superior = df.filter(pl.col('conta') == conta_superior)
            if linhas_conta_superior.height > 0:
                for i in range(0, linhas_conta_superior.height):
                    conta_cosif_superior = linhas_conta_superior.get_column('conta_cosif')[i]
                    if conta_cosif_superior != ValidacaoDesif.identificar_conta_superior(valor):
                        erros.append({"linha": num_linha, "reg": reg, "erro": 'EG039'})
        if valor not in ['7', '8']:
            erros.append({"linha": num_linha, "reg": reg, "erro": 'EG037'})
        result = df_cosif.filter(pl.col('conta') == valor)
        if result.height > 0:
            erros.append({"linha": num_linha, "reg": reg, "erro": 'EG036'})
        pattern = r'(\d{1})(\d{1})(\d{1})(\d{2})(\d{2})(\d+)'
        resultado = re.search(pattern, valor)
        if resultado.group(4) == '00':
            cosif_repetidas = df.filter(pl.col('conta_cosif') == valor)
            if cosif_repetidas.height > 1:
                erros.append({"linha": num_linha, "reg": reg, "erro": 'EG043'})
        return erros

    @staticmethod
    def validar_data(valor) -> bool:
        regex = r'^\d{4}(0[1-9]|1[0-2])$'
        return bool(re.match(regex, valor))

    @staticmethod
    def validar_numerico(s: str) -> bool:
        # Expressão regular para validar números inteiros ou decimais com vírgula
        regex = r'^-?\d+(,\d+)?$'
        return bool(re.match(regex, s))

    @staticmethod
    def validar_tamanho(valor, info_campo) -> bool:
        if valor is None:
            if info_campo.get('required'):
                return False
            return True
        tamanho = info_campo.get('length')
        tamanho_exato = info_campo.get('exact_length')
        length = len(str(valor))
        if tamanho_exato:
            if length != tamanho:
                return False
        else:
            if length > tamanho:
                return False
        return True

    @staticmethod
    def nao_numerico_ou_vazio(s: pl.Series) -> Series:
        return (~s.str.contains(r'^\d+$')) | (s.is_null())

    @staticmethod
    def eg030(valor, linha_anterior: pl.DataFrame):
        if valor.isdigit():
            des_anterior = linha_anterior.get_column('des_mista')[0]
            if des_anterior is not None and des_anterior.isdigit():
                if int(valor) - 1 == int(des_anterior):
                    return True
        return False

    @staticmethod
    def eg031(linha_seguinte: pl.DataFrame):
        des_seguinte = linha_seguinte.get_column('des_mista')[0]
        if des_seguinte is not None and des_seguinte.isdigit():
            if int(des_seguinte) == 2:
                return True
        return False

    @staticmethod
    def validar_conta_cosif_e_superior_em_contas_mistas(df: pl.DataFrame, reg: str):
        erros = []
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
                    num_linha = linhas.row(j)[0]
                    if linhas.row(j)[7] != conta_cosif:
                        erros.append({"linha": num_linha, "reg": reg, "erro": 'EG044'})
                    if linhas.row(j)[6] != conta_superior:
                        erros.append({"linha": num_linha, "reg": reg, "erro": 'EG050'})
            else:
                erros.append({"linha": linhas.row(0)[0], "reg": reg, "erro": 'EG032'})

        return erros

    @staticmethod
    def validar_conta_superior(valor: int | str, linha: pl.DataFrame, numero_linha: str, df: pl.DataFrame, reg: str):
        erros = []
        valor_conta = linha.get_column('conta')[0]
        if valor == valor_conta:
            erros.append({"linha": numero_linha, "Reg": reg, "erro": 'EG034'})
        if valor is None:
            grupo = linha.get_column('conta_cosif')[0][0]
            primeira_linha = \
                df.filter(pl.col('conta_cosif').str.starts_with(grupo)).row(0)[0]
            if numero_linha != primeira_linha:
                erros.append({"linha": numero_linha, "Reg": reg, "erro": 'EG042'})
        else:
            itens = df.filter(pl.col('conta') == valor)
            if itens.height > 1:
                for i in range(0, itens.height):
                    _linha = itens[i].row(0)[0]
                    if numero_linha <= _linha:
                        erros.append({"linha": numero_linha, "Reg": reg, "erro": 'EG049'})
            else:
                pass
                erros.append({"linha": numero_linha, "Reg": reg, "erro": 'EG033'})
        return erros

    @staticmethod
    def identificar_conta_superior(conta):
        pattern = r'(\d{1})(\d{1})(\d{1})(\d{2})(\d{2})(\d+)'
        resultado = re.search(pattern, conta)
        conta_superior = f"{resultado.group(1)}{resultado.group(2)}{resultado.group(3)}"
        if resultado.group(5) != '00':
            conta_superior = f"{conta_superior}{resultado.group(4)}00"
        elif resultado.group(4) != '00':
            conta_superior = f"{conta_superior}0000"

        dv = ValidacaoDesif.gerar_digito_verificador(conta_superior)
        return f"{conta_superior}{dv}"

    @staticmethod
    def gerar_digito_verificador(codigo):
        # Fatores de multiplicação
        fatores = [3, 7, 1, 3, 7, 1, 3, 7, 1]

        # Reverter o código para multiplicação da direita para esquerda
        codigo_revertido = codigo[::-1]

        # Inicializar soma
        soma = 0

        # Multiplicar cada dígito do código pelos fatores correspondentes
        for i in range(len(codigo_revertido)):
            digito = int(codigo_revertido[i])
            fator = fatores[i]
            soma += digito * fator

        # Obter o resto da divisão da soma por 10
        resto = soma % 10

        # Subtrair o resto de 10
        digito_verificador = 10 - resto

        # Caso o resto seja zero, o dígito verificador também é zero
        if resto == 0:
            digito_verificador = 0

        return digito_verificador
