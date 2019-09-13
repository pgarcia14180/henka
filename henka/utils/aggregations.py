def get_aggregation(**kwargs):
    field_name = ''
    for k, v in kwargs.items():
        field_name = v.replace('.keyword', '')+'_'+k
        return {
            field_name : {
                k: {
                    'field': v
                }
            }
        }
