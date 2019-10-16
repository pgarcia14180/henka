from .source import get_dataframe_from_source, get_dataframe_from_dataframe_source, JoinSource, AggSource, AggConcatenatedSource, SerieSource
from .process import DataframeProcessConfig
from .save import DataframeSave
from henka.utils.dictionary import DictToClass

class DataframeConfig:
    def __init__(self, **kwargs):
        self.name = kwargs['name']
        source_dict = kwargs['source']
        self.source_name = source_dict['name']
        if self.source_name == 'join':
            self.source = JoinSource(**source_dict)
        elif self.source_name == 'agg':
            self.source = AggSource(**source_dict)
        elif self.source_name == 'agg_concatenated':
            self.source = AggConcatenatedSource(**source_dict)
        elif self.source_name == 'serie':
            self.source = SerieSource(**source_dict)
        elif self.source_name == 'df':
            self.source = DictToClass(**source_dict)
        else:
            self.dataframe = get_dataframe_from_source(**kwargs['source'])
        self.process = DataframeProcessConfig(**kwargs['process'])
        if 'save' in kwargs:
            self.save = DataframeSave(_dataframe_name = self.name, **kwargs['save'])

    def set_df_from_dataframe(self, dataframes):
        self.dataframe = get_dataframe_from_dataframe_source(dataframes, self.source)
