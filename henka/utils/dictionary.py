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

class DictToClass:
    """
    Utility for creating classes from dictionaries.
    It createse the attribut source for returning the base dictionary.
    And has different methods for handling its content.
    The attribute required fields can be set optionally and it has to 
    be a list to point to the necessary fields for the class.
    """
    def __init__(self, **entries):
        self._source = dict(entries)
        self.__dict__.update(entries)
        self.check_for_required_fields(entries)

    def set_dict_and_attribute(self, key, value):
        self._source[key] = value
        setattr(self, key, value)

    def set_kwargs(self, **kwargs):
        for k, v in kwargs.items():
            self.set_dict_and_attribute(k, v)

    def check_for_required_fields(self, kwargs):
        if hasattr(self, 'required_fields'):
            for field in self.required_fields:
                if not field in kwargs:
                    raise AttributeError(f"{field} is required for instantiating this class")

    def remove_field(self, field):
        """
        Removes a field or a list of fields from source and instance.
        The argument can be the name of the field or a list with different names of fields
        """
        if isinstance(field, list):
            for f in field:
                del self._source[f]
                delattr(self, f)
        else:
            del self._source[field]
            delattr(self, field)

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
    Function that aplies a function or a list of functions to every single key in a dictionary
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
