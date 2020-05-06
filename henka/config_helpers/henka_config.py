from henka.utils.dictionary import DictClass
from .dataframes import DataframeConfig

class HenkaConfig:

    def __init__(self, *args):
        self.dataframes = [DataframeConfig(**df_conf) for df_conf in args]
