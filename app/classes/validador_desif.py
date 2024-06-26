import os
import re
import subprocess
import time
from app.helpers.helpers import criar_df, search_db
from app.models.municipio import Municipio
from app.models.codigo_tributacao import CodigoTributacao
from app.models.cosif_conta import CosifConta
from app.models.tarifa_bancaria import TarifaBancaria
from app.models.produto_servico import ProdutoServico
import polars as pl
from polars import Series
from typing import Optional
from datetime import datetime
from app.classes.identificacao_declaracao import IdentificacaoDeclaracao
from app.classes.plano_geral_contas_comentado import PlanoGeralContasComentado
from app.classes.tarifas_bancarias import TarifasBancarias
from app.classes.identificacao_outros_produtos_servicos import IdentificacaoOutrosProdutosServicos
from io import StringIO


class ValidacaoDesif:

    def __init__(self, caminho_arquivo: str, esquemas, leiautes):
        self.caminho_arquivo: str = caminho_arquivo
        self.arquivo = None
        self.conteudo: str = ""
        self.esquemas = esquemas
        self.leiautes = leiautes
        self.df: Optional[pl.DataFrame] = None
        self.lista_registros = None
        self.modulo: Optional[int] = None
        self.blocos_registros: Optional[dict[str, pl.DataFrame]] = None
        self.erros = []
        self.contas_cosif: Optional[pl.DataFrame] = None
        self.tarifas_bancarias: Optional[pl.DataFrame] = None
        self.outros_produtos_servicos: Optional[pl.DataFrame] = None
        self.conteudo_certificado: str = ""

    def validar(self) -> None:
        self.extrair_conteudo_arquivo()
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
        if self.modulo != '4':
            if self.ed035():
                ident_declaracao = IdentificacaoDeclaracao(self.blocos_registros['0000'], self.modulo, self)
                erros = ident_declaracao.validar()
                self.erros.extend(erros)
        if self.modulo == '3':
            self.ei030()
            if '0100' in self.lista_registros:
                pgcc = PlanoGeralContasComentado(self.blocos_registros['0100'], self)
                erros = pgcc.validar()
                self.erros.extend(erros)
            if '0200' in self.lista_registros:
                tarifas_bancarias = TarifasBancarias(self.blocos_registros['0200'], self)
                erros = tarifas_bancarias.validar()
                self.erros.extend(erros)
            if '0300' in self.lista_registros:
                pro_serv = IdentificacaoOutrosProdutosServicos(self.blocos_registros['0300'], self)
                erros = pro_serv.validar()
                self.erros.extend(erros)

    def extrair_conteudo_arquivo(self) -> None:
        # Comando OpenSSL para extrair o certificado e o conteúdo do arquivo .p7s
        extract_all_cmd = ["openssl", "smime", "-pk7out", "-inform", "DER", "-in", self.caminho_arquivo]

        # Executar o comando e capturar a saída
        result = subprocess.run(extract_all_cmd, capture_output=True, text=True)
        pkcs7_data = result.stdout

        if not pkcs7_data:
            raise ValueError("Falha ao extrair informações do arquivo.")

        # Comando OpenSSL para extrair o certificado do PKCS7 data
        extract_cert_cmd = ["openssl", "pkcs7", "-inform", "PEM", "-print_certs"]

        # Executar o comando e capturar a saída, passando pkcs7_data como entrada
        result = subprocess.run(extract_cert_cmd, input=pkcs7_data, capture_output=True, text=True)
        cert_data = result.stdout

        if not cert_data:
            raise ValueError("Certificado não encontrado ou falha ao extrair certificado.")

        # Comando OpenSSL para exibir as informações do certificado
        show_cert_cmd = ["openssl", "x509", "-noout", "-text"]

        # Executar o comando e capturar a saída, passando cert_data como entrada
        result = subprocess.run(show_cert_cmd, input=cert_data, capture_output=True, text=True)
        cert_info = result.stdout

        # Comando OpenSSL para extrair o conteúdo real do arquivo .p7s
        extract_content_cmd = ["openssl", "smime", "-verify", "-inform", "DER", "-in", self.caminho_arquivo, "-noverify",
                               "-outform", "PEM"]

        # Executar o comando e capturar a saída
        result = subprocess.run(extract_content_cmd, capture_output=True)
        content = result.stdout

        content = content.decode('utf-8', errors='ignore')

        if not content:
            raise ValueError("Conteúdo não encontrado ou falha ao extrair conteúdo.")

        self.conteudo_certificado = cert_data
        self.conteudo = content
        self.arquivo = StringIO(content)

    def eg018(self) -> bool:
        try:
            return os.path.getsize(self.caminho_arquivo) > 0
        except Exception as e:
            print("ERROR (EG018): Erro ao ler o arquivo: " + str(e))
            quit()

    def eg019(self) -> bool:
        try:
            self.df = pl.read_csv(self.arquivo, separator='|', encoding='utf8', has_header=False,
                                  schema_overrides=self.esquemas['padrao'], truncate_ragged_lines=True)
            return True
        except UnicodeDecodeError:
            self.erros.append({"erro": "EG019"})
        except Exception as e:
            self.erros.append("ERROR (EG019): Erro ao ler o arquivo: " + str(e))
        return False

    def definir_registros(self):
        if self.modulo == '3':
            data = {}
            for i in ['0000', '0100', '0200', '0300']:
                if self.df.filter(pl.col('column_2') == i).height > 0:
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
        for linha in self.conteudo.split('\n'):
            content = linha.split('|')
            if len(content) > 1 and content[1] in self.lista_registros:
                if len(content) - 1 != self.lista_registros[content[1]]['colunas']:
                    self.erros.append({'linha': content[0], 'reg': content[1], "erro": 'EG014'})

    def pegar_contas_cosif(self):
        if self.contas_cosif is None:
            self.contas_cosif = criar_df(CosifConta)
        return self.contas_cosif

    def pegar_tarifas_bancarias(self):
        if self.tarifas_bancarias is None:
            self.tarifas_bancarias = criar_df(TarifaBancaria)
        return self.tarifas_bancarias

    def pegar_outros_produtos_servicos(self):
        if self.outros_produtos_servicos is None:
            self.outros_produtos_servicos = criar_df(ProdutoServico)
        return self.outros_produtos_servicos

    def ei030(self) -> None:
        if str(self.modulo) == '3':
            data = self.df.filter(~pl.col('column_2').is_in(self.lista_registros.keys()))
            if data.height > 0:
                for i in range(0, data.height):
                    num_linha = data[i].select('column_1').row(0)[0]
                    reg = data[i].select('column_2').row(0)[0]
                    self.erros.append({"linha": num_linha, "reg": reg, "erro": 'EI030'})

    def verificar_registro(self, registro: str) -> bool:
        return registro in self.lista_registros.keys()

    def verificar_subtitulo_analitico(self, conta, linha: pl.DataFrame) -> bool:
        if '0100' in self.blocos_registros:
            contas_iguais = self.blocos_registros['0100'].filter(pl.col('conta') == conta)
            desdobro = linha.get_column('des_mista')[0]
            if contas_iguais.height > 1 and desdobro == '00':
                return False
            else:
                contas_filhas = self.blocos_registros['0100'].filter(pl.col('conta_supe') == conta)
                if contas_filhas.height > 0:
                    return False
            return True

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
        if resultado is not None:
            if resultado.group(4) == '00':
                cosif_repetidas = df.filter(pl.col('conta_cosif') == valor)
                if cosif_repetidas.height > 1:
                    erros.append({"linha": num_linha, "reg": reg, "erro": 'EG043'})
        return erros

    @staticmethod
    def validar_data(valor) -> bool:
        # Regex para validar datas em formato aaaamm ou aaaammdd
        regex = r'^\d{4}(0[1-9]|1[0-2])([0-2][0-9]|3[0-1])?$'
        return bool(re.match(regex, valor))

    @staticmethod
    def validar_numerico(s: str) -> bool:
        # Expressão regular para validar números inteiros ou decimais com vírgula
        regex = r'^-?\d+(,\d+)?$'
        return bool(re.match(regex, s))

    @staticmethod
    def validar_tamanho(valor, info_campo) -> bool:
        if valor is None:
            return not info_campo.get('required', True)

        tamanho_info = info_campo.get('length', '')
        is_tamanho_exato = info_campo.get('exact_length', False)
        valor_str = str(valor)

        if ',' in tamanho_info:
            max_length, decimal_places = map(int, tamanho_info.split(','))
            if ',' in valor_str:
                inteiro, decimal = valor_str.split(',')
                total_length = len(inteiro) + len(decimal)
            else:
                return False  # Número deveria ter uma vírgula e não tem

            if is_tamanho_exato:
                return total_length == max_length and len(decimal) == decimal_places
            else:
                return len(inteiro) <= max_length - decimal_places and len(decimal) <= decimal_places
        else:
            max_length = int(tamanho_info)
            total_length = len(valor_str)

            if is_tamanho_exato:
                return total_length == max_length
            else:
                return total_length <= max_length

    @staticmethod
    def nao_numerico_ou_vazio(s: pl.Series) -> Series:
        return (~s.str.contains(r'^\d+$')) | (s.is_null())

    @staticmethod
    def validar_desdobramentos(valor: str, linha: pl.DataFrame, loop: int, df: pl.DataFrame, num_linha: int | str,
                               reg: str):
        erros = []
        if valor is not None and valor.isdigit():
            if int(valor) >= 2 and loop > 0:
                linha_anterior = df[loop - 1]
                des_anterior = linha_anterior.get_column('des_mista')[0]
                if des_anterior is not None and des_anterior.isdigit():
                    if int(valor) - 1 != int(des_anterior):
                        erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG030'})
                else:
                    erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG030'})
            if int(valor) == 1:
                linha_seguinte = df[loop + 1]
                if linha_seguinte.height > 0:
                    des_seguinte = linha_seguinte.get_column('des_mista')[0]
                    if des_seguinte is not None and des_seguinte.isdigit():
                        if int(des_seguinte) != 2:
                            erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG031'})
                    else:
                        erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG031'})
                else:
                    erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG031'})
        if valor != "00":
            conta = linha.get_column('conta')[0]
            if conta[5:7] != '00':
                linhas = df.filter((pl.col('conta').str.starts_with(conta[:-1])) & (pl.col('conta') != conta))
                if linhas.height > 0:
                    erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG051'})
            else:
                erros.append({"linha": num_linha, "Reg": reg, "erro": 'EG051'})
        return erros

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

    @staticmethod
    def is_negativo(valor):
        return float(str(valor).replace(',', '.')) < 0

    def extrair_cnpj(self):
        # Ajustar a regex para buscar CNPJ após "LTDA:"
        cnpj_pattern = re.compile(r'LTDA:(\d{14})')
        match = cnpj_pattern.search(self.conteudo_certificado)

        if match:
            return match.group(1)
        else:
            return None

