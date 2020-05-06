import pandas as pd
from henka.utils.dictionary import DictClass
from henka.config_helpers.clients.mysql_client import MySqlClient

class MySqlSource(DictClass):

    def get_dataframe(self) -> pd.DataFrame:
        client = MySqlClient(**self)
        params = {}
        column_names = client.get_column_names()
        if column_names:
            params["columns"] = column_names
        return pd.DataFrame(client.execute_query(), **params )
