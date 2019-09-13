from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from henka.utils.index import create_dataframe_iterator
from henka.utils.search import get_response, get_search_response, get_multiple_queries, get_multiple_grouped_aggregations
from henka.utils.dictionary import DictToClass
from henka.utils.encoders import SetEncoder
from henka.utils.response import yield_agg_from_response

AGG_TYPES = ('agg_sum', 'agg_terms', 'agg_cardinality', 'agg_count')

class ESClientConfig(DictToClass):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = self.get_client()

    def get_client(self):
        return Elasticsearch(**self.get_client_context())

    def get_client_context(self):
        return {
            'host': self.url.strip(),
            'port': self.port,
            'scheme': self.scheme,
            'http_compress':True,
            'retry_on_timeout': True,
            'serializer': SetEncoder()
        }

    def get_body(self):
        """
        example: {sources: 'vta_retail, vta_transaction', tienda: 1, agg_sum: vta_retail}
        create an empty body to bring all results from index
        """
        body = {}
        agg_body = {}
        query_body = {}
        if bool(self._source['body']):
            for k, v in self._source['body'].items():
                if isinstance(v, str):
                    v = v if not ',' in v else v.split(',')
                if 'agg_size' in k:
                    agg_body.update({'size': v}) 
                elif 'fields' in k:
                    v = v if isinstance(v, list) else [v]
                    query_body.update({'_source': v})  
                elif 'es_range' in k:
                    query_body.update({'__range': v})  
                elif 'agg_sources' in k:
                    v = v if isinstance(v, list) else [v]
                    agg_body.update({'__sources': v})                
                elif 'query_size' in k:
                    body.update({'size': v})            
                elif k in AGG_TYPES:
                    k = k.replace('agg_', '')
                    agg_body.update({k: v})
                else:
                    query_body.update({k:v})
            query_body = get_multiple_queries(**query_body)
            agg_body = get_multiple_grouped_aggregations(**agg_body)
            if self.search_or_agg == 'search':
                return {**body, **query_body}
            else:
                return {**body, **agg_body, **query_body}
        else:
            return {"query": {"match_all": {}}}


    def get_data(self):
        data = []
        body = self.get_body()
        if (hasattr(self, 'body') and 'query_size' in self.body) or self.search_or_agg == 'agg':
            response = get_search_response(body, self.index_name, self.client)
        else:
            response = get_response(body, self.index_name, self.client)
        if self.search_or_agg == 'agg':
            aggregation_names = body['aggregations']['groupby']['aggregations'].keys()
            for agg in yield_agg_from_response(response, *aggregation_names):
                data.append(agg)
        elif self.search_or_agg == 'search' and 'query_size' in self.body:
            data = [hit['_source'] for hit in response['hits']['hits']]
        elif self.search_or_agg == 'search':
            data = [hit['_source'] for hit in response]
        return data


    def save_dataframe(self, df):
        results = bulk(self.client, create_dataframe_iterator(df, self.index_id, self.index_name), chunk_size = 10000, raise_on_error = False, request_timeout = 300)
        print(results)
