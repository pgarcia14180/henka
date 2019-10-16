import pandas as pd
from henka.config_helpers.henka_config import HenkaConfig 
from henka.processes.henka import henka 
from henka.utils.dates import datetime_to_str
from datetime import datetime
from dateutil import tz


def test_json():

    local_config = [
        {
            'name': 'pariscl',
            'source': {
                'name': 'json',
                'lines': True,
                'file_name': 'pariscl.json',
            },
            'process': {
                'keep_columns': [
                    #'customer_info', 
                    'order_no',
                    'order_total',
                    'payment_instruments',
                    'product_items',
                    #'c_rut',
                    'creation_date'
                    ],
                'rename_columns': {
                    'order_no': 'id_h',
                    'creation_date': 'fectrx'
                },
                'formulate': [
                    {
                        'function':lambda fectrx: pd.to_datetime(fectrx.astype(str)[:19], format='%Y-%m-%dT%H:%M:%S'),
                        'result': 'fectrx'
                    },
                    {
                        'function': lambda fectrx: fectrx.dt.strftime('%Y%m%d').astype(int),
                        'result': 'fectrx_n8'
                    }
                ],
                'duplicate_columns': {
                    'fectrx': ['fectrx_dt'],
                    'id_h': ['numtrantsl'],
                    'fectrx_n8': ['fectrantsl']
                },

            },
            'save': {
                'name': 'csv',
                'file_name': 'pariscl.csv',
            }
        },
        {
            'name': 'payment_instruments',
            'source': {
                'name': 'serie',
                'dataframe_name': 'pariscl',
                'serie_name': 'payment_instruments',
                'is_nested_list': True,
                'serie_index': 'id_h',
            },
            'process': {
                'rename_columns': {
                    'id_h': 'id_h',
                    'amount': "monpagtsl",
                    'payment_instrument_id': 'id'
                }
            },
            'save': {
                'name': 'csv',
                'file_name': 'payment_instruments.csv',
            }
        },
        {
            'name': 'payment_card',
            'source': {
                'name': 'serie',
                'dataframe_name': 'payment_instruments',
                'serie_name': 'payment_card',
                'serie_index': 'id',
            },
            'process': {
                'rename_columns': {
                    'number_last_digits': 'bin'
                }
            },
            'save': {
                'name': 'csv',
                'file_name': 'payment_card.csv',
            }
        },
        {
            'name': 'txd_header',
            'source': {
                'name': 'df',
                'dataframe_name': 'pariscl',
                },
            'process': {
                'process_content': {
                },
                'keep_columns': [
                    'id_h',
                    'fectrx',
                    'fectrx_n8',
                    'fectrantsl',
                    'fecttrx_dt',
                    'numtrantsl'
                ],
                'mock': {
                    'tienda': 32,
                    'cadena': 1
                },
            },                
            'save': {
                'name': 'csv',
                'file_name': 'txd_header.csv',
            }
        },
        {
            'name': 'product_items',
            'source': {
                'name': 'serie',
                'dataframe_name': 'pariscl',
                'serie_name': 'product_items',
                'is_nested_list': True,
                'serie_index': 'id_h',
            },
            'process': {
                'rename_columns': {
                    'product_id': 'sku',
                    'price': 'totarttsl',
                    'quantity': 'canarttsl',
                    'item_id': 'id'
                },
                'keep_columns': [
                    'sku',
                    'totarttsl',
                    'canarttsl',
                    'id',
                    'id_h',
                ]
            },
            'save': {
                'name': 'csv',
                'file_name': 'product_items.csv',
            }
        },
        {
            'name': 'txd_item',
            'source': {
                'name': 'join',
                'dataframe_left': 'product_items',
                'dataframe_right': 'txd_header',
                'how': 'left',
                'left_on': 'id_h',
                'right_on': 'id_h',
                },
            'process': {
            },
            'save': {
                'name': 'csv',
                'file_name': 'txd_item.csv',
            }
        },
        {
            'name': 'txd_mpago_no_payment_card',
            'source': {
                'name': 'join',
                'dataframe_left': 'payment_instruments',
                'dataframe_right': 'txd_header',
                'how': 'left',
                'left_on': 'id_h',
                'right_on': 'id_h',
                },
            'process': {
                'process_content': {
                },
            },
            'save': {
                'name': 'csv',
                'file_name': 'txd_mpago_no_payment_card.csv',
            }
        },
        {
            'name': 'txd_mpago',
            'source': {
                'name': 'join',
                'dataframe_left': 'payment_card',
                'dataframe_right': 'txd_mpago_no_payment_card',
                'how': 'right',
                'left_on': 'id',
                'right_on': 'id',
                },
            'process': {
                'keep_columns': {
                        'id',
                        'bin',
                        'monpagtsl',
                        'id_h',
                        'fectrx',
                        'fectrx_n8',
                        'numtrantsl',
                        'fectrantsl',
                        'tienda',
                        'cadena',
                    }
            },
            'save': {
                'name': 'csv',
                'file_name': 'txd_mpago.csv',
            }
        },
    ]
    henka(HenkaConfig(*local_config))

    

