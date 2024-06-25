from polars.datatypes import Utf8

esquema_padrao = {
    'column_1': Utf8,
    'column_2': Utf8,
    'column_3': Utf8,
    'column_4': Utf8,
    'column_5': Utf8,
    'column_6': Utf8,
    'column_7': Utf8,
    'column_8': Utf8,
    'column_9': Utf8,
    'column_10': Utf8,
    'column_11': Utf8,
    'column_12': Utf8,
    'column_13': Utf8,
    'column_14': Utf8,
    'column_15': Utf8,
}
esquema_0000 = {
    'num_linha': Utf8,
    'reg': Utf8,
    'cnpj': Utf8,
    'nome': Utf8,
    'tipo_inti': Utf8,
    'cod_munc': Utf8,
    'ano_mes_inic_cmpe': Utf8,
    'ano_mes_fim_cmpe': Utf8,
    'modu_decl': Utf8,
    'tipo_decl': Utf8,
    'prtc_decl_ante': Utf8,
    'tipo_cnso': Utf8,
    'cnpj_resp_rclh': Utf8,
    'idn_versao': Utf8,
    'tipo_arred': Utf8
}
esquema_0100 = {
    'num_linha': Utf8,
    'reg': Utf8,
    'conta': Utf8,
    'des_mista': Utf8,
    'nome': Utf8,
    'desc_conta': Utf8,
    'conta_supe': Utf8,
    'conta_cosif': Utf8,
    'cod_trib_des_if': Utf8
}
esquema_0200 = {
    'num_linha': Utf8,
    'reg': Utf8,
    'idto_tari': Utf8,
    'dat_vige': Utf8,
    'val_tari_unit': Utf8,
    'val_tari_perc': Utf8,
    'sub_titu': Utf8,
    'des_mista': Utf8
}
esquema_0300 = {
    'num_linha': Utf8,
    'reg': Utf8,
    'idto_serv': Utf8,
    'desc_compl_serv': Utf8,
    'sub_titu': Utf8,
    'des_mista': Utf8
}

esquemas = {
    '0000': esquema_0000,
    '0100': esquema_0100,
    '0200': esquema_0200,
    '0300': esquema_0300,
    'padrao': esquema_padrao
}
leiautes = {
    "0000": {
        "num_linha": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": False
        },
        "reg": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": True
        },
        "cnpj": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": True
        },
        "nome": {
            "required": True,
            "type": "string",
            "length": '100',
            "exact_length": False
        },
        "tipo_inti": {
            "required": True,
            "type": "string",
            "length": '1',
            "exact_length": True
        },
        "cod_munc": {
            "required": True,
            "type": "number",
            "length": '7',
            "exact_length": True
        },
        "ano_mes_inic_cmpe": {
            "required": True,
            "type": "date",
            "length": '6',
            "exact_length": True
        },
        "ano_mes_fim_cmpe": {
            "required": True,
            "type": "date",
            "length": '6',
            "exact_length": True
        },
        "modu_decl": {
            "required": True,
            "type": "number",
            "length": '1',
            "exact_length": True
        },
        "tipo_decl": {
            "required": True,
            "type": "number",
            "length": '1',
            "exact_length": True
        },
        "prtc_decl_ante": {
            "required": False,
            "type": "string",
            "length": '30',
            "exact_length": False
        },
        "tipo_cnso": {
            "required": False,
            "type": "number",
            "length": '1',
            "exact_length": True
        },
        "cnpj_resp_rclh": {
            "required": False,
            "type": "number",
            "length": '6',
            "exact_length": True
        },
        "idn_versao": {
            "required": True,
            "type": "string",
            "length": '10',
            "exact_length": False
        },
        "tipo_arred": {
            "required": False,
            "type": "number",
            "length": '1',
            "exact_length": True
        }
    },
    "0100": {
        "num_linha": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": False
        },
        "reg": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": True
        },
        "conta": {
            "required": True,
            "type": "string",
            "length": '30',
            "exact_length": False
        },
        "des_mista": {
            "required": True,
            "type": "number",
            "length": '2',
            "exact_length": True
        },
        "nome": {
            "required": True,
            "type": "string",
            "length": '100',
            "exact_length": False
        },
        "desc_conta": {
            "required": False,
            "type": "string",
            "length": '600',
            "exact_length": False
        },
        "conta_supe": {
            "required": False,
            "type": "string",
            "length": '30',
            "exact_length": False
        },
        "conta_cosif": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": True
        },
        "cod_trib_des_if": {
            "required": False,
            "type": "number",
            "length": '9',
            "exact_length": False
        }
    },
    "0200": {
        "num_linha": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": False
        },
        "reg": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": True
        },
        "idto_tari": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": False
        },
        "dat_vige": {
            "required": True,
            "type": "date",
            "length": '8',
            "exact_length": True
        },
        "val_tari_unit": {
            "required": True,
            "type": "number",
            "length": '8,2',
            "exact_length": False
        },
        "val_tari_perc": {
            "required": True,
            "type": "number",
            "length": '5,2',
            "exact_length": False
        },
        "sub_titu": {
            "required": True,
            "type": "string",
            "length": '30',
            "exact_length": False
        },
        "des_mista": {
            "required": True,
            "type": "number",
            "length": '2',
            "exact_length": True
        },
    },
    "0300": {
        "num_linha": {
            "required": True,
            "type": "number",
            "length": '8',
            "exact_length": False
        },
        "reg": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": True
        },
        "idto_serv": {
            "required": True,
            "type": "number",
            "length": '4',
            "exact_length": False
        },
        "desc_compl_serv": {
            "required": False,
            "type": "string",
            "length": '255',
            "exact_length": False
        },
        "sub_titu": {
            "required": True,
            "type": "string",
            "length": '30',
            "exact_length": False
        },
        "des_mista": {
            "required": True,
            "type": "number",
            "length": '2',
            "exact_length": True
        },
    },
}
