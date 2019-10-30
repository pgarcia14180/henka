import pandas as pd
import numpy as np
from collections.abc import Iterable
from functools import reduce
from henka.utils.dictionary import DictToClass
from .clients import ESClientConfig
from henka.utils.dataframes import dataframe_to_dict
from itertools import chain
from henka.utils.loggers import report


class ESSource(DictToClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def get_dataframe(self) -> pd.DataFrame:
        client = ESClientConfig(**self._source)
        return pd.DataFrame(client.get_data())
    
class ExcelSource(DictToClass):
    
    def get_dataframe(self):
        df = pd.read_excel(self.file_name, error_bad_lines=False, sheet_name = self.sheet_name)
        if hasattr(self, 'columns'):
            df = df[self.columns]
        return df

class CSVSource(DictToClass):
    
    def get_dataframe(self) -> pd.DataFrame:
        read_csv_args = {
        }
        if hasattr(self, 'separator'):
            read_csv_args['sep'] = self.separator
        if hasattr(self, 'columns'):
            read_csv_args['usecols'] = self.columns
        if hasattr(self, 'encoding'):
            read_csv_args['encoding'] = self.encoding
        return pd.read_csv(self.file_name, error_bad_lines=False,  **read_csv_args)

class JSONSource(DictToClass):
       
    def get_dataframe(self) -> pd.DataFrame:
        args = {}
        if hasattr(self, 'lines'):
            args['lines'] = self.lines
        return pd.read_json(self.file_name, **args)

class AggSource(DictToClass):
    pass

class AggConcatenatedSource(DictToClass):
    pass

class JoinSource(DictToClass):
    pass

class SerieSource(DictToClass):
    pass

def get_source_instance(**kwargs):
    if kwargs['name'] == 'es' or kwargs['name'] == 'elasticsearch':
        return ESSource(**kwargs)
    if kwargs['name'] == 'excel':
        return ExcelSource(**kwargs)
    if kwargs['name'] == 'csv':
        return CSVSource(**kwargs)
    if kwargs['name'] == 'json':
        return JSONSource(**kwargs)
    

def get_dataframe_from_source(**kwargs):
    return get_source_instance(**kwargs).get_dataframe()

def get_dataframe_from_dataframe_source(dataframes_dict, source):
    if source.name == 'join':
        return pd.merge(dataframes_dict[source.dataframe_left], dataframes_dict[source.dataframe_right], how= source.how, left_on = source.left_on, right_on = source.right_on)
    if source.name == 'agg':
        agg = dataframes_dict[source.dataframe_name].groupby(source.groupby).agg(source.agg_type).reset_index()
        return  agg
    if source.name == 'agg_concatenated':
        dfs = []
        for concatenated_column in source.concatenated_columns:
            dfs.append(dataframes_dict[source.dataframe].groupby(source.groupby)[concatenated_column].apply(lambda x: source.separator.join(x)).reset_index())
        return reduce(lambda df1, df2: pd.merge(df1, df2, how='inner', on = source.groupby), dfs)
    if source.name == 'df':
        return dataframes_dict[source.dataframe_name]
    if source.name == 'serie':
        source_df = dataframes_dict[source.dataframe_name]
        if hasattr(source, 'is_nested_list') and source.is_nested_list:
            def add_key_to_nested_list_of_dict(serie, *indices):
                dictionary_list = []
                try:
                    for dictionary in serie:
                        for i in range(1, len(indices), 2):
                            dictionary.update({indices[i-1]: indices[i]})
                            dictionary_list.append(dictionary)
                except:
                    pass
                return dictionary_list
            index_name = [source.serie_index] if isinstance(source.serie_index, str) else source.serie_index
            index_serie = [source_df[source.serie_index]] if isinstance(source.serie_index, str) else [source_df[serie_index] for serie_index in source.serie_index]
            index_joined = [index_item for index_set in zip(index_name, index_serie) for index_item in index_set]
            #report(np.vectorize(add_key_to_nested_list_of_dict)(source_df[source.serie_name], *index_joined))
            df = pd.DataFrame(list(chain(*np.vectorize(add_key_to_nested_list_of_dict)(source_df[source.serie_name], *index_joined))))
        else:
            if hasattr(source, 'serie_index'):
                def add_key_to_dict(serie, *indices):
                    if isinstance(serie, dict):
                        for i in range(1, len(indices), 2):
                            serie.update({indices[i-1]: indices[i]})
                        return serie
                    else:
                        return {}
                index_name = [source.serie_index] if isinstance(source.serie_index, str) else source.serie_index
                index_serie = [source_df[source.serie_index]] if isinstance(source.serie_index, str) else [source_df[serie_index] for serie_index in source.serie_index]
                index_joined = [index_item for index_set in zip(index_name, index_serie) for index_item in index_set]
                df = pd.DataFrame(list(np.vectorize(add_key_to_dict)(source_df[source.serie_name], *index_joined)))
            else:
                df = dataframes_dict[source.dataframe_name][source.serie_name].apply(pd.Series)
        return df

