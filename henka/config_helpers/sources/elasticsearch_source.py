import pandas as pd
from henka.utils.dictionary import DictClass
from henka.config_helpers.clients.elasticsearch_client import ElasticSearchClient

class ElasticsearchSource(DictClass):
    
    def get_dataframe(self) -> pd.DataFrame:
        client = ElasticSearchClient(**self)
        return pd.DataFrame(client.get_data())
    