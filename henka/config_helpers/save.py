import os
import pandas as pd
from henka.utils.dictionary import DictClass
from henka.utils.dataframes import dataframe_to_dict
from henka.config_helpers.clients.elasticsearch_client import ElasticSearchClient
from henka.config_helpers.clients.mysql_client import MySqlClient
from openpyxl import load_workbook

class DataframeSave(DictClass):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.name == 'elasticsearch' or self.name == 'es':
            self.client = ElasticSearchClient(**self)
        if self.name == 'mysql':
            self.client = MySqlClient(**self)
            

    def save_dataframe(self, df):
        if self.name == 'elasticsearch' or self.name == 'es':
            self.save_to_elastic(df)
        if self.name == 'excel':
            self.save_to_excel(df)
        if self.name == 'csv':
            self.save_to_csv(df)
        if self.name == 'pickle':
            self.save_to_pickle(df)
        if self.name == "mysql":
            self.save_to_mysql(df)
        
    def save_to_elastic(self, df):
        self.client.save_dataframe(df)

    def save_to_excel(self, df: pd.DataFrame):
        if not "file_name" in self:
            self.file_name = self._dataframe_name+'.xlsx'
        if os.path.exists(self.file_name) and 'append_sheet' in self:
            excel_file = pd.ExcelWriter(self.file_name, engine='openpyxl')
            book = load_workbook(self.file_name)
            excel_file.book = book
        else:
            excel_file = self.file_name
        if not "sheet_name" in self:
            self.sheet_name = self._dataframe_name 
        df.to_excel(excel_file, sheet_name= self.sheet_name)
        if not isinstance(excel_file, str):
            excel_file.save()
            excel_file.close()

    def save_to_csv(self, df: pd.DataFrame):
        params = {
            'index' : False
        }
        header = True
        if not 'file_name' in self:
            csv_file = self._dataframe_name+'.csv'
        else:
            csv_file = self.file_name
        if 'header' in self and self.header == False:
            params['header'] = False
        if 'separator' in self and self.separator:
            params['sep'] = self.separator
        if 'append' in self and self.append and os.path.isfile(csv_file):
            params['mode'] = 'a'
            params['header'] = False
        df.to_csv(csv_file, **params)

    def save_to_pickle(self, df: pd.DataFrame):
        df.to_pickle(self.file_name)

    def save_to_mysql(self, df: pd.DataFrame):
        df = df.fillna("")
        columns = ",".join(df.columns.values)
        values = ""
        for row in dataframe_to_dict(df):
            values += "({}),\n".format(",".join([str(value) if not isinstance(value, str) else f"'{value.strip()}'" for value in row.values()]))
        values = values[:-2]
        save_query = f"insert into {self.table_name} ({columns}) \n values \n {values}"
        if "is_update" in self and self.is_update:
            if "id" in self:
                table_id = self.id
            elif "id" in df.columns.values:
                table_id = "id"
            else:
                table_id = df.columns.values[0]
            save_query += " ON DUPLICATE KEY UPDATE " + ",".join([
                f"{column}=VALUES({column})" for column in df.columns.values if not column == table_id
                ])+"\n"
        self.client.query = save_query
        self.client.execute_query()
        self.client.commit()            
            

