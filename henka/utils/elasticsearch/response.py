def yield_agg_pagination(search_body, index_name, client):
    after_key = None
    while True:
        if after_key:
            search_body['aggregations']['groupby']['composite']['after'] = after_key
        response = client.search(
            body = search_body, 
            doc_type = 'doc', 
            index = index_name,
            request_timeout=300,
        )
        if not len(response['aggregations']['groupby']['buckets']) == 0:
            after_key = response['aggregations']['groupby']['after_key']
            yield response
        else:
            break

def yield_agg_from_response(response, *aggregation_names):
    print(response['aggregations']['groupby']['buckets'])
    for agg in response['aggregations']['groupby']['buckets']:
        agg_dict = {}
        for k, v in agg.items():
            print(k, v)
            if k == 'key':
                agg_dict.update(**v)
            if k in aggregation_names:
                if 'value' in v:
                    agg_dict[k] = v['value']
        yield agg_dict
