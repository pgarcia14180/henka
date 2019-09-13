def yield_group_by(dict_list, key):
    grouped_dict = {}
    for dict_item in dict_list:
        if dict_item.get(key) in grouped_dict:
            grouped_dict[dict_item.get(key)].append(dict_item)
        else:
            grouped_dict[dict_item.get(key)] = [dict_item]
    for k, v in grouped_dict.items():
        yield (k, v)
