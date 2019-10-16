from henka.config_helpers.henka_config import HenkaConfig 
from henka.processes.henka import henka 

def test_formulate():

    local_config = [
        {
            'name': 'items',
            'drop': True,
            'source': {
                'name': 'csv',
                'file_name': 'pcf_test.csv'
            },
            'process': {
                'formulate': [
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'col1',
                    },
                    {   
                        'function': lambda col1, col2: col1+col2,
                        'result': 'result',
                    },
                    {   
                        'function': lambda col1, result: result+col1,
                        'result': 'result2',
                    },
                    {   
                        'function': lambda col1, col2, col3, col4, result, result2: (col1+col2+col3+result+result2/col1)-col4,
                        'result': 'total',
                    },
                    {   
                        'function': lambda total: round(total//2),
                        'result': 'total'
                    },
                    

                ],
                'delete_columns': ['col2', 'col3', 'col4', 'result'],
                'rename_columns': {
                    'col1': 'base'
                },
            },
            'save': {
                'name': 'csv',
                'file_name': 'result.csv',
            }
        }

    ]
    henka(HenkaConfig(*local_config))
