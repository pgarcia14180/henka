from henka.utils.dictionary import DictToClass
from henka.utils.numbers import convert_to_int
from henka.utils.text import (
    remove_first_character, 
    to_title, 
    convert_to_string, 
    correct_text_coding,
    strip_text,
    unidecode)

functions  = {
    'remove_first_character': remove_first_character,
    'convert_to_int': convert_to_int,
    'to_title': to_title,
    'convert_to_string': convert_to_string,
    'correct_text_coding': correct_text_coding,
    'strip_text': strip_text,
    'unidecode': unidecode
}

class DataframeProcessConfig(DictToClass):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.set_kwargs(function_config = functions)
