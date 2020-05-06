import pandas as pd
from henka.utils.dictionary import DictClass
from henka.config_helpers.mixins.file_mixin import FileMixin
from henka.config_helpers.mixins.download_mixin import DownloadMixin

class JSONSource(FileMixin, DownloadMixin, DictClass):
       
    def get_dataframe(self) -> pd.DataFrame:
        args = {}
        args['lines'] = self.pop('lines', None)
        # mejorar esto
        df = pd.read_json(self.file_name, **args)
        if not self.get("remove_files") == False: self.remove_local_files()
        return df