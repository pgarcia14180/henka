import sys
import pandas as pd
from henka.utils.dictionary import DictClass, trim_dictionary
from henka.config_helpers.mixins.download_mixin import DownloadMixin
from henka.config_helpers.mixins.file_mixin import FileMixin


class CSVSource(FileMixin, DownloadMixin, DictClass):

    def create_df(self, file_name, **read_csv_args):
        try:
            if 'separator' in read_csv_args: read_csv_args["sep"] = read_csv_args["separator"]
            if not "encondig" in read_csv_args: read_csv_args["encoding"] = "ISO-8859-1"
            df =  pd.read_csv(file_name, error_bad_lines=False, **read_csv_args)
            if hasattr(self, "remove_files"): self.remove_file(file_name)
        except:
            if not file_name:
                print("No file to read")
            else:
                print("Error with file", file_name)
                print(sys.exc_info()[0])
            df = pd.DataFrame()
        return df

    def get_dataframe(self) -> pd.DataFrame:
        self.download()
        read_csv_args = {
        }
        if hasattr(self, 'separator'):
            read_csv_args['sep'] = self.separator
        if hasattr(self, 'columns'):
            read_csv_args['usecols'] = self.columns
        if hasattr(self, 'encoding'):
            read_csv_args['encoding'] = self.encoding
        if hasattr(self, "file_name"):
            return self.create_df(self.file_name, **read_csv_args)
        else:
            dfs = [self.create_df(file_name, **read_csv_args) for file_name in self.yield_files_names_from_dir()]
            if len(dfs) == 1:
                return dfs[0]
            else:
                return pd.concat(dfs) 
