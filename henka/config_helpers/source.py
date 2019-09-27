import pandas as pd
from functools import reduce
from henka.utils.dictionary import DictToClass
from .clients import ESClientConfig

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
    pass

class AggSource(DictToClass):
    pass

class AggConcatenatedSource(DictToClass):
    pass

class JoinSource(DictToClass):
    pass

def get_source_instance(**kwargs):
    if kwargs['name'] == 'es' or kwargs['name'] == 'elasticsearch':
        return ESSource(**kwargs)
    if kwargs['name'] == 'excel':
        return ExcelSource(**kwargs)
    if kwargs['name'] == 'csv':
        return CSVSource(**kwargs)

def get_dataframe_from_source(**kwargs):
    return get_source_instance(**kwargs).get_dataframe()

def get_dataframe_from_dataframe_source(dataframes_dict, source):
    if source.name == 'join':
        return pd.merge(dataframes_dict[source.dataframe_left], dataframes_dict[source.dataframe_right], how= source.how, left_on = source.left_on, right_on=source.right_on).drop_duplicates(subset=None, keep='first').dropna()
    if source.name == 'agg':
        return  getattr(dataframes_dict[source.dataframe].groupby(source.groupby), source.agg_type)().reset_index().dropna()
    if source.name == 'agg_concatenated':
        dfs = []
        for concatenated_column in source.concatenated_columns:
            dfs.append(dataframes_dict[source.dataframe].groupby(source.groupby)[concatenated_column].apply(lambda x: source.separator.join(x)).reset_index())
        return reduce(lambda df1, df2: pd.merge(df1, df2, how='inner', on = source.groupby), dfs)
