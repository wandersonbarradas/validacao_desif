import polars as pl
from datetime import datetime
from app.helpers.helpers import search_db
from app.models.titulo_bancario import TituloBancario
from dateutil.relativedelta import relativedelta


class IdentificacaoDeclaracao:
    def __init__(self, reg0000: pl.DataFrame, modulo, validacao_desif):
        self.reg0000: pl.DataFrame = reg0000
        self.modulo = str(modulo)
        self.erros = []
        self.validacao_desif = validacao_desif

    def validar(self):
        self.ed004_ed005_ed023_ed052_ed054_eg007()
        self.ed012_ed031()
        self.ed013_ed021_ed048_ed051()
        self.ed015()
        self.ed037()
        self.loop_campos()
        if self.modulo == '3':
            if not self.validacao_desif.verificar_registro('0200'):
                self.erros.append({'linha': '1', 'Reg': '0000', 'erro': 'EI024'})
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
            if self.validacao_desif.validar_data(valor_ano_mes_inic_cmpe) is False:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'EG007'})
                return
            if self.validacao_desif.validar_data(valor) is False:
                self.erros.append({"linha": linha, "Reg": '0000', "erro": 'EG007'})
                return
            data_limite = self.validacao_desif.criar_data((datetime.now() - relativedelta(years=10)).strftime("%Y%m")).date()
            data_atual = self.validacao_desif.criar_data((datetime.now()).strftime("%Y%m")).date()
            data_fim_competencia = self.validacao_desif.criar_data(valor).date()
            data_inicio_competencia = self.validacao_desif.criar_data(valor_ano_mes_inic_cmpe).date()
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
        resultado = self.validacao_desif.validar_municipio(valor)
        if not resultado:
            self.erros.append({"linha": num_linha, "Reg": '0000', "erro": 'EG001'})

    def loop_campos(self):
        linha = self.reg0000[0]
        numero_linha = linha.get_column('num_linha')[0]
        nome_colunas = linha.columns
        info_campos = self.validacao_desif.leiautes['0000']
        for nome_campo in nome_colunas:
            info_campo = info_campos.get(nome_campo)
            valor = linha.get_column(nome_campo)[0]
            if info_campo.get('type') == 'number' and valor is not None and not self.validacao_desif.validar_numerico(
                    str(valor)):
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG008'})
            if not self.validacao_desif.validar_tamanho(valor, info_campo):
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG009'})
            if info_campo.get('required') and valor is None:
                self.erros.append({"linha": numero_linha, "Reg": '0000', "erro": 'EG046'})
            if nome_campo == 'cod_munc':
                self.eg001(valor, numero_linha)
                self.ed059(valor, numero_linha)  # TODO: Puxar tabela de configurações
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

    def ed059(self, valor, num_linha):
        if valor is not None and valor != '2203305':
            self.erros.append({"Linha": num_linha, "Reg": '0000', "Erro": 'ED059'})

