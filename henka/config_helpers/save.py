import os
import pandas as pd
from henka.utils.dictionary import DictToClass
from .clients import ESClientConfig
from openpyxl import load_workbook

class DataframeSave(DictToClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name == 'elasticsearch' or self.name == 'es':
            self.client = ESClientConfig(**self._source)
            

    def save_dataframe(self, df):
        if self.name == 'elasticsearch' or self.name == 'es':
            self.save_to_elastic(df)
        if self.name == 'excel':
            self.save_to_excel(df)
        if self.name == 'csv':
            self.save_to_csv(df)
        

    def save_to_elastic(self, df):
        self.client.save_dataframe(df)

    def save_to_excel(self, df: pd.DataFrame):
        if os.path.exists(self.file_name):
            excel_file = pd.ExcelWriter(self.file_name, engine='openpyxl')
            book = load_workbook(self.file_name)
            excel_file.book = book
        else:
            excel_file = self.file_name
        df.to_excel(excel_file, sheet_name= self.sheet_name)
        if not isinstance(excel_file, str):
            excel_file.save()
            excel_file.close()

    def save_to_csv(self, df: pd.DataFrame):
        params = {
            'index' : False
        }
        header = True
        csv_file = self.file_name
        if hasattr(self, 'header') and not self.header:
            params['header'] = False
        if hasattr(self, 'separator'):
            params['sep'] = self.separator
        if hasattr(self, 'append') and self.append and os.path.isfile(csv_file):
            params['mode'] = 'a'
        df.to_csv(csv_file, **params)
