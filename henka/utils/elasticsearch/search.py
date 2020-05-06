from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

def get_search_all_body():
    return {
    "query": {
        "match_all": {}
        }
    }

def q(**kwargs):
    body = {}
    for key, value in kwargs.items():
        body[key] = value
    return body

def get_query_by_multiple_values(name, values):
    return q(
       query = q(
           bool=q(
               must=[
                    q(
                        terms={name: [*values]}
                    )
               ]
           )
       )
    )

def get_source(name, size = None):
    no_keyword_name = name.replace('.keyword', '')
    body = {
        no_keyword_name: {
            "terms": {
            "field": name,
            "missing_bucket": True,
            }
        }
    }
    return body

def get_aggregation(**kwargs):
    field_name = ''
    for k, v in kwargs.items():
        if not k == 'size':
            field_name = v.replace('.keyword', '')+'_'+k
            body = {
                field_name : {
                    k: {
                        'field': v
                    }
                }
            }
            if 'size' in kwargs and k == 'terms':
                body[field_name][k]['size'] = int(kwargs['size'])
            return body

def get_exclude_query_by_multiple_values(name, values):
    return q(
       query = q(
           bool=q(
               must_not=[
                    q(
                        terms={name: [*values]}
                    )
               ]
           )
       )
    )

def get_aggregation(**kwargs):
    field_name = ''
    for k, v in kwargs.items():
        if not k == 'size':
            field_name = v.replace('.keyword', '')+'_'+k
            body = {
                field_name : {
                    k: {
                        'field': v
                    }
                }
            }
            if 'size' in kwargs and k == 'terms':
                body[field_name][k]['size'] = int(kwargs['size'])
            return body

def get_multiple_queries(**kwargs):
    body = {'query': {'bool': {'must': [], 'must_not': []}, 'range': {}}}
    has_range = False
    for k, v in kwargs.items():
        if '__keyword' in k:
            k = k.replace('__keyword', '.keyword')
        if '__range' in k:
            body['query']['bool']['filter'] = [{'bool': {'must': {'range': v}}}]
        elif k == '_source':
            body['_source'] = v
        elif not k[0] == '_':
            if isinstance(v, list):
                body['query']['bool']['must'].append({'terms': {k: v}})
            else:
                body['query']['bool']['must'].append({'term': {k: v}})
        else:
            k = k[1:]
            if isinstance(v, list):
                body['query']['bool']['must_not'].append({'terms': {k: v}})
            else:
                body['query']['bool']['must_not'].append({'term': {k: v}})
    to_delete_fields = ['must', 'must_not', '__range']
    for field in to_delete_fields:
        if field in body['query']['bool']:
            if not body['query']['bool'][field]:
                del body['query']['bool'][field]
    if not body['query']['range']:
        del body['query']['range']
    return body

def get_multiple_grouped_aggregations(**kwargs):
    body = {"aggregations": {
        "groupby": {
            "composite": {
                "size": 10000,
                "sources": [],},
            "aggregations": {}}}}
    if 'size' in kwargs:
        size_dict = {'size': kwargs['size']}
    else:
        size_dict = {}
    for k, v in kwargs.items():
        if not k == 'size':
            if k == '__sources':
                for source in v:
                    body['aggregations']['groupby']['composite']['sources'].append(get_source(source))
            elif k == 'terms':
                if isinstance(v, list):
                    for _v in v:
                        terms_dict = {k: _v}
                        body['aggregations']['groupby']['aggregations'] = get_aggregation(**terms_dict, **size_dict)
                else:
                    terms_dict = {k: v}
                    terms_dict['size'] = kwargs['size']
                    
                    body['aggregations']['groupby']['aggregations'] = get_aggregation(**terms_dict, **size_dict)
            else:
                if isinstance(v, list):
                    for _v in v:
                        terms_dict = {k: _v}
                        body['aggregations']['groupby']['aggregations'].update(get_aggregation(**{k: _v}, **size_dict))
                else:
                    body['aggregations']['groupby']['aggregations'] = get_aggregation(**{k: v}, **size_dict)
    return body

def get_response(body, index, client):
    return scan(
        client = client,
        scroll= '35m',
        raise_on_error= True,
        index= index,
        query = body,
        doc_type = "doc",
        request_timeout = 300
        
        )

def get_search_response(search_body, index, client):
    return client.search(
            body = search_body, 
            doc_type = 'doc', 
            index = index,
            request_timeout=300,
        )

def yield_agg_pagination(search_body, config):
    client = config.get_client()
    date = config.get_date(week_date = True)
    after_key = None
    while True:
        if after_key:
            search_body['aggregations']['groupby']['composite']['after'] = after_key
        response = client.search(
            body = search_body, 
            doc_type = 'doc', 
            index = 'mdh_item_upd-'+date,
            request_timeout=300,
        )
        if not len(response['aggregations']['groupby']['buckets']) == 0:
            after_key = response['aggregations']['groupby']['after_key']
            yield response
        else:
            break
