from henka.config_helpers.henka_config import HenkaConfig
from henka.processes.henka import henka
import sys

def process_cybers(date, date2):
    date2 = date2 if date2 else date
    local_config = [
        {
            'name': 'upd',
            'source': {
                'name': 'elasticsearch',
                'index_name': f'txd_item_upd-{str(date)[:6]}*',
                'url': 'url',
                'port': '9200',
                'scheme': 'http',
                'body': {
                    '_source': ['id_h', 'sku', 'sku_desc', 'vta_retail', 'vta_tsc', 'h2_desc', 'h5_desc'],
                    'tienda': 32,
                    'es_range': {'fectrx_n8':{'gte': date, 'lte': date2}},
                    },
                'search_or_agg': 'search'
                },
            },
            'save': {
                'name': 'csv',
                'file_name': 'upd.csv',
            }
        },
        {
            'name': 'mpago',
            'source': {
                'name': 'elasticsearch',
                'index_name': f'txd_mpago-{str(date)[:6]}*',
                'url': 'url',
                'port': '9200',
                'scheme': 'http',
                'body': {
                    '_source': ['id_h', 'ruttcb', 'desc_codpagotsl'],
                    'numctltsl': 32,
                    'es_range': {'fectrantsl':{'gte': date, 'lte': date2}},
                    },
                'search_or_agg': 'search'
                },
            'process': {
                'remove_duplicates': ['id_h'],
            }
        },
        {
            'name': 'sku_client',
            'source': {
                'name': 'join',
                'dataframe_left': 'upd',
                'dataframe_right': 'mpago',
                'how': 'left',
                'left_on': 'id_h',
                'right_on': 'id_h',
                },
            'process': {
                'process_content': {
                    #'remove_first_character': ['Codigo_de_tienda'],
                    'convert_to_string': ['ruttcb'],
                    #'convert_to_int': ['Codigo_de_tienda', 'Prefijo_Tienda'],
                },
            },
            'save': {
                'name': 'csv',
                'file_name': 'client_sku.csv',
            }
        },
        {
            'name': 'client_sku_concatenated',
            'source': {
                'name': 'agg_concatenated',
                'dataframe': 'sku_client',
                'separator': ' , ',
                'concatenated_columns': [ 'ruttcb', 'desc_codpagotsl'],
                'groupby': ['sku', 'sku_desc', 'h2_desc', 'h5_desc', 'id_h']
                },
            'save': {
                'name': 'csv',
                'file_name': 'client_sku_concat.csv',
            }
        },
        {
            'name': 'sku_sum',
            'source': {
                'name': 'agg',
                'dataframe': 'sku_client',
                'agg_type': 'sum',
                'groupby': ['sku', 'sku_desc', 'h2_desc', 'h5_desc', 'id_h']
                },
            'save': {
                'name': 'csv',
                'file_name': 'client_sku_sum.csv',
            }
        },
        {
            'name': 'sku_client_final',
            'source': {
                'name': 'join',
                'dataframe_left': 'client_sku_concatenated',
                'dataframe_right': 'sku_sum',
                'how': 'right',
                'left_on': ['sku', 'sku_desc', 'h2_desc', 'h5_desc', 'id_h'],
                'right_on': ['sku', 'sku_desc', 'h2_desc', 'h5_desc', 'id_h'],
                },
            'save': {
                'name': 'csv',
                'append': True,
                'file_name': f'pariscl_{str(date)[:6]}.csv',
            }
        },
    ]
    config = HenkaConfig(*local_config)
    henka(config)

date = sys.argv[1]
date2 = sys.argv[1]

process_cybers(date,date2)
