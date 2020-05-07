import sys
import pandas as pd
import numpy as np
import json
from collections.abc import Iterable
from functools import reduce
from henka.utils.dictionary import DictClass
from itertools import chain
from henka.config_helpers.sources.csv_source import CSVSource
from henka.config_helpers.sources.excel_source import ExcelSource
from henka.config_helpers.sources.json_source import JSONSource
from henka.config_helpers.sources.mysql_source import MySqlSource
from henka.config_helpers.sources.elasticsearch_source import ElasticsearchSource




def get_source_instance(**kwargs):
    if kwargs['name'] == 'es' or kwargs['name'] == 'elasticsearch':
        return ElasticsearchSource(**kwargs)
    if kwargs['name'] == 'excel':
        return ExcelSource(**kwargs)
    if kwargs['name'] == 'csv':
        return CSVSource(**kwargs)
    if kwargs['name'] == 'json':
        return JSONSource(**kwargs)
    if kwargs['name'] == "mysql":
        return MySqlSource(**kwargs)

def get_dataframe_from_source(**kwargs):
    return get_source_instance(**kwargs).get_dataframe()

def get_dataframe_from_dataframe_source(dataframes_dict, source):
    if source.name == 'join':
        return pd.merge(dataframes_dict[source.dataframe_left], dataframes_dict[source.dataframe_right], how= source.how, left_on = source.left_on, right_on = source.right_on)
    if source.name == 'agg':
        if 'group_by' in source:
            source.groupby = source.group_by
        if 'groupby' in source:
            agg = dataframes_dict[source.dataframe_name].groupby(source.groupby).agg(source.agg_type).reset_index()
        if isinstance(source.agg_type, list) or isinstance(source.agg_type, dict):
            agg.columns = ['_'.join(column).strip() if column[1] else column[0] for column in agg.columns.values]
        return  agg
    if source.name == 'agg_concatenated':
        dfs = []
        for concatenated_column in source.concatenated_columns:
            dfs.append(dataframes_dict[source.dataframe].groupby(source.groupby)[concatenated_column].apply(lambda x: source.separator.join(x)).reset_index())
        return reduce(lambda df1, df2: pd.merge(df1, df2, how='inner', on = source.groupby), dfs)
    if source.name == "df_concatenated":
        return pd.concat([dataframes_dict[df_name] for df_name in source.dfs])
    if source.name == 'df' or source.name == "dataframe":
        return dataframes_dict[source.dataframe_name]
    #todo: rename to json_series
    if source.name == 'series':
        source_df = dataframes_dict[source.dataframe_name]
        if hasattr(source, 'is_nested_list') and source.is_nested_list:
            def add_key_to_nested_list_of_dict(series, *indices):
                dictionary_list = []
                #todo: proper exception handling
                try:
                    for dictionary in series:
                        for i in range(1, len(indices), 2):
                            dictionary.update({indices[i-1]: indices[i]})
                            dictionary_list.append(dictionary)
                except:
                    pass
                return dictionary_list
            index_name = [source.series_index] if isinstance(source.series_index, str) else source.series_index
            index_series = [source_df[source.series_index]] if isinstance(source.series_index, str) else [source_df[series_index] for series_index in source.series_index]
            index_joined = [index_item for index_set in zip(index_name, index_series) for index_item in index_set]
            #report(np.vectorize(add_key_to_nested_list_of_dict)(source_df[source.series_name], *index_joined))
            df = pd.DataFrame(list(chain(*np.vectorize(add_key_to_nested_list_of_dict)(source_df[source.series_name], *index_joined))))
        else:
            if hasattr(source, 'series_index'):
                def add_key_to_dict(series, *indices):
                    if isinstance(series, str):
                        series = json.loads(series)
                    if isinstance(series, dict):
                        for i in range(1, len(indices), 2):
                            series.update({indices[i-1]: indices[i]})
                        return series
                    else:
                        return {}
                index_name = [source.series_index] if isinstance(source.series_index, str) else source.series_index
                index_series = [source_df[source.series_index]] if isinstance(source.series_index, str) else [source_df[series_index] for series_index in source.series_index]
                index_joined = [index_item for index_set in zip(index_name, index_series) for index_item in index_set]
                df = pd.DataFrame(list(np.vectorize(add_key_to_dict)(source_df[source.series_name], *index_joined)))
            else:
                df = dataframes_dict[source.dataframe_name][source.series_name].apply(pd.Series)
        return df

