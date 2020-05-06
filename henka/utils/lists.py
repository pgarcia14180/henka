def yield_group_by(dict_list, key):
    grouped_dict = {}
    for dict_item in dict_list:
        if dict_item.get(key) in grouped_dict:
            grouped_dict[dict_item.get(key)].append(dict_item)
        else:
            grouped_dict[dict_item.get(key)] = [dict_item]
    for k, v in grouped_dict.items():
        yield (k, v)

def chunks(origin_list, size):
    """
    yield parts of a list according to the size designed
    """
    for i in range(0, len(origin_list), size):
        yield origin_list[i:i + size]