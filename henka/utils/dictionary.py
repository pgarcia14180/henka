def get_map_dictionary_function(field_mapper_dictionary):
    """
    Function that takes as argument a dictionary for creating a mapper of keys
    to another dictionary.
    """
    def generic_dictionary_mapper(source_dict):
        nonlocal field_mapper_dictionary
        body = {}
        for k, v in source_dict.items():
            for k2, v2 in field_mapper_dictionary.items():
                if k == k2:
                    if isinstance(v2, list):
                        for _v in v2:
                            body[_v] = v
                    else:
                        body[v2] = v
        return body
    return generic_dictionary_mapper

class DictClass(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def process_all_dictionary_content(dictionary, function):
    """
    Function that aplies a function or a list of functions to every single value in a dictionary
    """
    for k, v in dictionary.items():
        if isinstance(function, list):
            for f in function:
                dictionary[k] = f(v)
        else:
            dictionary[k] = function(v)
    return dictionary


def process_all_dictionary_keys(dictionary, function):
    """
    Function that applies a function or a list of functions to every single key in a dictionary
    """
    _dictionary = {}
    for k, v in dictionary.items():
        if isinstance(function, list):
            for f in function:
                _dictionary[f(k)] = v
        else:
            _dictionary[function(k)] = v
    return _dictionary

def get_process_dictionary_content_function(**kwargs):
    """
    Function that takes the name of the dictionary field and maps a function to its value.

    The keyword __all applies a function or list of functions to all values.
    """
    def process_dictionary_content(dictionary):
        nonlocal kwargs
        for k, v in kwargs.items():
            for dk, dv in dictionary.items():
                if k == dk:
                    dictionary[k] = v(dv)
            if k == '__all':
                dictionary = process_all_dictionary_content(dictionary, v)
        return dictionary
    return process_dictionary_content

def get_dictionary_expander_function(*function_list):
    def dictionary_expander_function(**kwargs):
        nonlocal function_list
        for function in function_list:
            kwargs = function(**kwargs)
        return kwargs
    return dictionary_expander_function

def trim_dictionary(dictionary, *args):
    return {k: dictionary.get(k) for k in args}

def unpack_dictionary_values(dictionary, *args):
    dictionary_values = []
    for arg in args:
        dictionary_values.append(dictionary.get(arg))
    return dictionary_values

def search_dictionary_value(dictionary, key):
    result_list = []
    for k, v in dictionary.items():
        if k == key and v: result_list.append(v)
    for k, v in dictionary.items():
        if isinstance(v, dict):
            result_list += search_dictionary_value(v, key)
        elif isinstance(v, list):
            for part in v:
                result_list += search_dictionary_value(part, key)
    return result_list

def search_key_in_nested_structure(structure, key):
    result_list = []
    if isinstance(structure, list):
        for part in structure:
            result_list += search_key_in_nested_structure(part, key)
    if isinstance(structure, dict):
        result_list += search_dictionary_value(structure, key)
    return result_list

def navigate_in_nested_structure(structure, *keys):
    result = structure
    for k in keys:
        result = search_key_in_nested_structure(result, k)
    return result
