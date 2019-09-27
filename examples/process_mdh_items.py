import time
from henka.config_helpers.henka_config import HenkaConfig as  MainConfig
from henka.processes.henka import henka as dataprocess
from henka.utils.encoders import SetEncoder
from elasticsearch import Elasticsearch
from henka.utils.loggers import report

replace_content_dict = {
    'Ã\x91': 'ñ',
    'Ã\x93': 'ó',
    'Ã\x8d': 'í',
    'Ã\x81': 'á',
    'Ã\x89': 'é',
    'Ã\x80': 'ó',
    'Ã\x92': 'ó',
    'Ã\x9a': 'ú',
   }

def process_main_scan_cd(main_scan_cd):
    if len(str(main_scan_cd)) > 2:
        return int(str(main_scan_cd)[:-1])
    else:
        return int(main_scan_cd)


def process_mdh_items():
    local_config = [
        {
            'name': 'structure',
            'source': {
            'name': 'excel',
            'file_name': 'Estructura Comercial.xlsx',
            'sheet_name': 'Tablas',
            'columns': ['Seccion', 'Rubro', 'Nombre_Seccion', 'Nombre_Rubro', 'Gerencia_Linea']
            },
            'process': {
                'duplicate_columns': {'Gerencia_Linea': 'h1_code'},
                'replace_content': {
                    'h1_code': {
                        'Otra': 1,
                        'Hogar': 2,
                        'Ferretería': 3,
                        'Jardin y Outdoors': 4,
                        'Terminaciones': 5,
                        'Construcción': 6,
                    }
                },
                'process_content': {
                    'convert_to_int': ['h1_code'],
                },
                'rename_columns': {
                    'Gerencia_Linea': 'h1_desc',
                    'Nombre_Seccion': 'h2_desc',
                    'Seccion': 'h2_code',
                    'Nombre_Rubro': 'h3_desc',
                },
            },
            'save': {
                'name': 'csv',
                'file_name': 'structure.csv',
            }
        },
        {
            'name': 'items',
            'source': {
                'name': 'csv',
                'file_name': 'items.csv',
                'columns': [
                    'item_id',
                    'short_desc',
                    'main_scan_cd',
                    'comercial_line_name',                    
                    'item_hierarchy_level_1_cd', 'item_hierarchy_level_2_cd', 'item_hierarchy_level_3_cd', 'item_hierarchy_level_4_cd', 
                    'item_hierarchy_level_3_desc',
                    'item_hierarchy_level_4_desc',
                    ],
            },
            'process': {
                'replace_content':  {
                    'item_hierarchy_level_3_desc': replace_content_dict,
                    'item_hierarchy_level_4_desc': replace_content_dict,
                },
                'process_content': {
                    'to_title': [
                        'short_desc',
                        'item_hierarchy_level_3_desc',
                        'item_hierarchy_level_4_desc',
                    ]
                },
                'process_content_custom': [
                    {   
                        'function': process_main_scan_cd,
                        'arguments': ['main_scan_cd'],
                        'results': ['main_scan_cd'],
                    }
                ],
                'rename_columns': {
                    'short_desc': 'sku_desc',
                    'main_scan_cd': 'sku',
                    'item_hierarchy_level_2_cd': 'h3_code', 
                    'item_hierarchy_level_3_cd': 'h4_code', 
                    'item_hierarchy_level_4_cd': 'h5_code', 
                    'item_hierarchy_level_3_desc': 'h4_desc',
                    'item_hierarchy_level_4_desc': 'h5_desc',
                },
            }
        },
        {
            'name': 'items_final',
            'source': {
                'name': 'join',
                'dataframe_left': 'items',
                'dataframe_right': 'structure',
                'how': 'left',
                'left_on': 'h3_code',
                'right_on': 'Rubro',
                },
            'process': {
                'process_content': {
                    'convert_to_int': ['h1_code'],
                },
                'delete_columns': [
                    'Rubro'
                    ],
            },
            'save': {
                'name': 'csv',
                'file_name': 'results_multiple.csv',
            }
        },
    ]


    config = MainConfig(*local_config)
    dfs = dataprocess(config)



    
        