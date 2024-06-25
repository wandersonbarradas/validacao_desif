import polars as pl


class IdentificacaoOutrosProdutosServicos:

    def __init__(self, reg0300: pl.DataFrame, validacao_desif):
        self.reg0300: pl.DataFrame = reg0300
        self.erros = []
        self.validacao_desif = validacao_desif

    def validar(self):
        self.loop_campos()
        return self.erros

    def loop_campos(self):
        for i in range(0, self.reg0300.height):
            linha = self.reg0300[i]
            numero_linha = linha.get_column('num_linha')[0]
            nome_colunas = linha.columns
            info_campos = self.validacao_desif.leiautes['0300']
            for nome_campo in nome_colunas:
                info_campo = info_campos.get(nome_campo)
                valor = linha.get_column(nome_campo)[0]
                # Validações padrões
                if (info_campo.get('type') == 'number'
                        and valor is not None
                        and not self.validacao_desif.validar_numerico(str(valor))):
                    self.erros.append({"linha": numero_linha, "Reg": '0300', "erro": 'EG008'})

                if not self.validacao_desif.validar_tamanho(valor, info_campo):
                    self.erros.append({"linha": numero_linha, "Reg": '0300', "erro": 'EG009'})
                if info_campo.get('required') and valor is None:
                    self.erros.append({"linha": numero_linha, "Reg": '0300', "erro": 'EG046'})
                # Validações dos campos
                if nome_campo == 'idto_serv':
                    self.ei017(valor, numero_linha)
                if nome_campo == 'desc_compl_serv':
                    if valor is None:
                        self.ei022(linha, numero_linha)
                if nome_campo == 'sub_titu':
                    if valor is not None:
                        self.ei019(valor, linha, numero_linha)
                        self.ei025(valor, linha, numero_linha)
                        self.ei026(valor, linha, numero_linha)
                        if valor[0] == '8':
                            self.erros.append({"linha": numero_linha, "Reg": '0300', "erro": 'EI034'})

    def ei017(self, valor, num_linha):
        outros_prod_serv = self.validacao_desif.pegar_outros_produtos_servicos().filter(pl.col('codigo') == valor)
        if outros_prod_serv.height == 0:
            self.erros.append({"linha": num_linha, "reg": '0300', "erro": 'EI017'})

    def ei019(self, valor, linha: pl.DataFrame, num_linha):
        des_mista = linha.get_column('des_mista')[0]
        id_serv = linha.get_column('idto_serv')[0]
        servicos = self.reg0300.filter(
            (pl.col('idto_serv') == id_serv) & (pl.col('sub_titu') == valor) & (pl.col('des_mista') == des_mista)
        )
        if servicos.height > 1:
            self.erros.append({"linha": num_linha, "reg": '0300', "erro": 'EI019'})

    def ei022(self, linha: pl.DataFrame, num_linha):
        id_serv = linha.get_column('idto_serv')[0]
        serv_db: pl.DataFrame = self.validacao_desif.pegar_outros_produtos_servicos().filter(pl.col('codigo') == id_serv)
        print(serv_db)
        if serv_db.height > 0:
            if serv_db.get_column('descricao_complementar_obrigatoria')[0] == '1':
                self.erros.append({"linha": num_linha, "reg": '0300', "erro": 'EI022'})

    def ei025(self, valor, linha: pl.DataFrame, num_linha):
        is_analitico = self.validacao_desif.verificar_subtitulo_analitico(valor, linha)
        if not is_analitico:
            self.erros.append({"linha": num_linha, "reg": '0300', "erro": 'EI025'})

    def ei026(self, valor, linha: pl.DataFrame, num_linha):
        if '0100' in self.validacao_desif.blocos_registros:
            des_mista = linha.get_column('des_mista')[0]
            contas = self.validacao_desif.blocos_registros['0100'].filter(
                (pl.col('conta') == valor) & (pl.col('des_mista') == des_mista))
            if contas.height == 0:
                self.erros.append({"linha": num_linha, "reg": '0300', "erro": 'EI026'})
