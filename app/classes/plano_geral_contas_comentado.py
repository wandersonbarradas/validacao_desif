import polars as pl
import re


class PlanoGeralContasComentado:

    def __init__(self, reg0100: pl.DataFrame, validacao_desif):
        self.reg0100: pl.DataFrame = reg0100
        self.erros = []
        self.validacao_desif = validacao_desif

    def validar(self):
        self.loop_campos()
        self.eg032_eg044_eg050()  # TODO: Pesada
        self.ei001()
        self.ei023()
        return self.erros

    def loop_campos(self):
        for i in range(0, self.reg0100.height):
            linha = self.reg0100[i]
            numero_linha = linha.get_column('num_linha')[0]
            nome_colunas = linha.columns
            info_campos = self.validacao_desif.leiautes['0100']
            for nome_campo in nome_colunas:
                info_campo = info_campos.get(nome_campo)
                valor = linha.get_column(nome_campo)[0]
                # Validações padrões
                if info_campo.get(
                        'type') == 'number' and valor is not None and not self.validacao_desif.validar_numerico(
                        str(valor)):
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG008'})
                if not self.validacao_desif.validar_tamanho(valor, info_campo):
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG009'})
                if info_campo.get('required') and valor is None:
                    self.erros.append({"linha": numero_linha, "Reg": '0100', "erro": 'EG046'})
                # Validações dos campos
                if nome_campo == 'conta':
                    pass
                    self.ei028(valor, linha, numero_linha)  # TODO: Pesada
                if nome_campo == 'des_mista':
                    pass
                    self.eg030_eg031_eg051(valor, linha, i, numero_linha)  # TODO: Pesada
                if nome_campo == 'desc_conta':
                    self.ei004(valor, linha, numero_linha)
                if nome_campo == 'conta_supe':
                    pass
                    self.eg033_eg034_eg042_eg049(valor, linha, numero_linha)  # TODO: Pesada
                if nome_campo == 'conta_cosif':
                    pass
                    self.eg036_eg037_eg039_eg043(valor, linha, numero_linha)  # TODO: Pesada
                if nome_campo == 'cod_trib_des_if':
                    if valor is not None:
                        self.eg010_eg011(valor, linha, numero_linha)

    def eg010_eg011(self, valor: int | str, linha: pl.DataFrame, num_linha: int | str):
        result = self.validacao_desif.validar_cod_tributacao(valor)
        if not result:
            self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EG011'})
        conta = linha.get_column('conta')[0]
        is_analitico = self.validacao_desif.verificar_subtitulo_analitico(conta, linha)
        if not is_analitico:
            self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EI010'})

    def eg030_eg031_eg051(self, valor: str, linha: pl.DataFrame, loop: int, num_linha: int | str):
        erros = self.validacao_desif.validar_desdobramentos(valor, linha, loop, self.reg0100, num_linha, '0100')
        if len(erros) > 0:
            self.erros.extend(erros)

    def eg032_eg044_eg050(self):
        erros = self.validacao_desif.validar_conta_cosif_e_superior_em_contas_mistas(self.reg0100, '0100')
        if len(erros) > 0:
            self.erros.extend(erros)

    def eg033_eg034_eg042_eg049(self, valor, linha, numero_linha):
        erros = self.validacao_desif.validar_conta_superior(valor, linha, numero_linha, self.reg0100, '0100')
        if len(erros) > 0:
            self.erros.extend(erros)

    def eg036_eg037_eg039_eg043(self, valor, linha: pl.DataFrame, num_linha):
        errors = self.validacao_desif.validar_conta_cosif(valor, linha, self.reg0100,
                                                          self.validacao_desif.pegar_contas_cosif(), num_linha, '0100')
        if len(errors) > 0:
            self.erros.extend(errors)

    def ei028(self, valor, linha: pl.DataFrame, num_linha):
        if valor is not None:
            conta_cosif = linha.get_column('conta_cosif')[0]
            if conta_cosif[5:7] == '00':
                contas_inferiores = self.reg0100.filter(pl.col('conta_supe') == valor)
                if contas_inferiores.height == 0:
                    cosifs_inferiores = self.validacao_desif.pegar_contas_cosif().filter(
                        pl.col("conta_superior").str.replace_all('.', "", literal=True).str.replace_all('-', "",
                                                                                                        literal=True) == conta_cosif)
                    if cosifs_inferiores.height > 0:
                        self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EI028'})

    def ei004(self, valor, linha: pl.DataFrame, num_linha):
        if valor is None:
            conta = linha.get_column('conta')[0]
            pattern = r'(\d{1})(\d{1})(\d{1})(\d{2})(\d{2})(\d+)'
            resultado = re.search(pattern, conta)
            if resultado.group(5) != '00':
                self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EI004'})
            elif resultado.group(4) != '00':
                contas_filhas = self.reg0100.filter(pl.col('conta_supe') == conta)
                if contas_filhas.height == 0:
                    self.erros.append({"linha": num_linha, "Reg": '0100', "erro": 'EI004'})

    def ei001(self):
        df_duplicados = self.reg0100.groupby("conta", "des_mista").agg(pl.count("conta").alias("contagem"))
        df_duplicados = df_duplicados.filter(pl.col("contagem") > 1)
        df_linhas_repetidas = self.reg0100.join(df_duplicados, on=["conta", "des_mista"], how="inner")
        if df_linhas_repetidas.height > 1:
            for i in range(0, df_linhas_repetidas.height):
                self.erros.append({"linha": df_linhas_repetidas.row(i)[0], "Reg": '0100', "erro": 'EI001'})

    def ei023(self):
        contas = self.reg0100.filter(pl.col('conta_cosif').str.starts_with('7'))
        if contas.height == 0:
            self.erros.append({'linha:': '1', 'Reg': '0100', 'erro': 'EI023'})
