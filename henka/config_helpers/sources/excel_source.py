import pandas as pd
from henka.utils.dictionary import DictClass
from henka.config_helpers.mixins.file_mixin import FileMixin
from henka.config_helpers.mixins.download_mixin import DownloadMixin
from xlrd.compdoc import CompDocError
import OleFileIO_PL

class ExcelSource(FileMixin, DownloadMixin, DictClass):
    
    def get_dataframe(self):
        self.download()
        df = None
        try:
            df = pd.read_excel(self.file_name, **self)
        except CompDocError:
            with open(self.file_name,'rb') as file:
                ole = OleFileIO_PL.OleFileIO(file)
                if ole.exists('Workbook'):
                    stream = ole.openstream('Workbook')
                    df = pd.read_excel(stream, **self)

        if hasattr(self, 'columns'):
            df = df[self.columns]
        if hasattr(self, "remove_local_files"): self.remove_local_files()
        return df
