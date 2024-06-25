import polars as pl
from datetime import datetime


class TarifasBancarias:

    def __init__(self, reg0200: pl.DataFrame, validacao_desif):
        self.reg0200: pl.DataFrame = reg0200
        self.erros = []
        self.validacao_desif = validacao_desif

    def validar(self):
        self.loop_campos()
        return self.erros

    def loop_campos(self):
        for i in range(0, self.reg0200.height):
            linha = self.reg0200[i]
            numero_linha = linha.get_column('num_linha')[0]
            nome_colunas = linha.columns
            info_campos = self.validacao_desif.leiautes['0200']
            for nome_campo in nome_colunas:
                info_campo = info_campos.get(nome_campo)
                valor = linha.get_column(nome_campo)[0]
                # Validações padrões
                if info_campo.get(
                        'type') == 'number' and valor is not None and not self.validacao_desif.validar_numerico(
                    str(valor)):
                    self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EG008'})

                if not self.validacao_desif.validar_tamanho(valor, info_campo):
                    self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EG009'})
                if info_campo.get('required') and valor is None:
                    self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EG046'})
                # Validações dos campos
                if nome_campo == 'idto_tari':
                    if valor is not None:
                        self.ei037(valor, numero_linha)
                if nome_campo == 'dat_vige':
                    if valor is not None:
                        self.ei038(valor, numero_linha)
                if nome_campo == 'val_tari_unit' and valor is not None:
                    if self.validacao_desif.is_negativo(valor):
                        self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EG048'})
                if nome_campo == 'val_tari_perc' and valor is not None:
                    if self.validacao_desif.is_negativo(valor):
                        self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EG048'})
                if nome_campo == 'sub_titu':
                    if valor is not None:
                        self.ei002(valor, linha, numero_linha)
                        self.ei013(valor, linha, numero_linha)
                        self.ei015(valor, linha, numero_linha)
                        if valor[0] == '8':
                            self.erros.append({"linha": numero_linha, "Reg": '0200', "erro": 'EI034'})

    def ei002(self, valor, linha: pl.DataFrame, num_linha):
        is_analitico = self.validacao_desif.verificar_subtitulo_analitico(valor, linha)
        if not is_analitico:
            self.erros.append({"linha": num_linha, "reg": '0200', "erro": 'EI002'})

    def ei013(self, valor, linha: pl.DataFrame, num_linha):
        if '0100' in self.validacao_desif.blocos_registros:
            des_mista = linha.get_column('des_mista')[0]
            contas = self.validacao_desif.blocos_registros['0100'].filter(
                (pl.col('conta') == valor) & (pl.col('des_mista') == des_mista))
            if contas.height == 0:
                self.erros.append({"linha": num_linha, "reg": '0200', "erro": 'EI013'})

    def ei015(self, valor, linha: pl.DataFrame, num_linha):
        des_mista = linha.get_column('des_mista')[0]
        id_tarifa = linha.get_column('idto_tari')[0]
        tarifas = self.reg0200.filter(
            (pl.col('idto_tari') == id_tarifa) & (pl.col('sub_titu') == valor) & (pl.col('des_mista') == des_mista)
        )
        if tarifas.height > 1:
            self.erros.append({"linha": num_linha, "reg": '0200', "erro": 'EI015'})

    def ei037(self, valor, num_linha):
        tarifas = self.validacao_desif.pegar_tarifas_bancarias().filter(pl.col('codigo') == (valor if len(valor) > 3 else '0' + valor))
        if tarifas.height == 0:
            self.erros.append({"linha": num_linha, "reg": '0200', "erro": 'EI037'})

    def ei038(self, valor, num_linha):
        if not self.validacao_desif.validar_data(valor):
            self.erros.append({"linha": num_linha, "Reg": '0200', "erro": 'EG005'})
            return
        data_vigencia = self.validacao_desif.criar_data(valor).date()
        valor_fim_declaracao = self.validacao_desif.blocos_registros['0000'].get_column('ano_mes_fim_cmpe')[0]
        data_fim_competencia = self.validacao_desif.criar_data(valor_fim_declaracao).date()
        data_atual = self.validacao_desif.criar_data((datetime.now()).strftime("%Y%m")).date()
        if data_vigencia > data_fim_competencia or data_vigencia > data_atual:
            self.erros.append({"linha": num_linha, "Reg": '0200', "erro": 'EI038'})

