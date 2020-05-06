from .source import get_dataframe_from_source, get_dataframe_from_dataframe_source
from .process import DataframeProcessConfig
from .save import DataframeSave
from henka.utils.dictionary import DictClass
from henka.utils.constants import sources_from_dataframes

class DataframeConfig(DictClass):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        source_dict = self.pop("source")
        self.source_name = source_dict['name']
        if self.source_name in sources_from_dataframes:
            self.source = DictClass(**source_dict)
        else:
            self.dataframe = get_dataframe_from_source(**source_dict)
        if self.get("process"): self.process = DataframeProcessConfig(**self.process)
        if self.get("save"): self.save = DataframeSave(_dataframe_name = self.name, **self.save)

    def set_df_from_dataframe(self, dataframes):
        self.dataframe = get_dataframe_from_dataframe_source(dataframes, self.source)
