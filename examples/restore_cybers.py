from henka.config_helpers.henka_config import HenkaConfig as  MainConfig
from henka.processes.henka import henka as dataprocess

def restore_cyber(date):
    config = [{
        'name': 'upd',
        'source': {
            'name': 'elasticsearch',
            'index_name': 'txd_cyber',
            'url': 'localhost',
            'port': '9200',
            'scheme': 'http',
            'body': {
                    'es_range': {'fectrx_n8':{'gte': date, 'lte': date}},
                    'tienda': 32
                    },
            'search_or_agg': 'search'
            },
        'process': {
            'mock': {
                'vta_transaction': 'Y',
            }
        },
        'save': {
            'name': 'elasticsearch',
            'index_name': 'txd_item_upd'+'-'+date,
            'index_id': 'id',
            'url': 'localhost',
            'port': '9200',
            'scheme': 'http',
        }
    }]
    dataprocess(MainConfig(*config))