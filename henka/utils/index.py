import numpy as np
import math
from henka.utils.loggers import report
import csv

def create_index_body_list_iterator(index_body_list, index_name, id = 'id'):
    for index_body in index_body_list:
        index_id = index_body[id]
        yield {
            '_op_type': 'index',
            '_index':index_name,
            '_type': 'doc',
            '_id': index_id,
            '_source': index_body
        }

def create_dataframe_iterator(df, index_id, index_name, op_type = 'index'):
    dictionary_list = []
    for row in df.keys():
        for i, column in enumerate(df[row]):
            if not isinstance(column, str):
                try:
                    if math.isnan(column):
                        column = None
                except TypeError:
                    pass
            try:
                dictionary_list[i].update({row: column})
            except IndexError:
                dictionary_list.append({row: column})

    for body in dictionary_list:
        if i % 10000 == 0:
            report(i)
        _index_id = body[index_id]
        yield {
            '_op_type': op_type,
            '_index':index_name,
            '_type': 'doc',
            '_id': _index_id,
            '_source': body
        }

def create_csv_iterator(index_name, file_name, index_id):
    f = open(file_name, 'rU' )  
    reader = csv.DictReader(f)  
    for i, index_body in enumerate(reader):
        _index_id = None
        del index_body['']
        for k, v in index_body.items():
            if k == index_id:
                _index_id = v
            if v.isdigit():
                try:
                    index_body[k] = float(v)
                except ValueError:
                    index_body[k] = int(v)
        if i % 10000 == 0:
            report(i)
        yield {
            '_op_type': 'index',
            '_index':index_name,
            '_type': 'doc',
            '_id': _index_id,
            '_source': index_body
        }
