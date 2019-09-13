from unidecode import unidecode_expect_nonascii

def correct_text_coding(text):
    try:
        return bytes(text, 'cp1252').decode('utf-8')
    except UnicodeDecodeError:
        return text

def unidecode(text):
    if isinstance(text, str):
        text = unidecode_expect_nonascii(text)
    return text

def remove_last_character(text_or_int):
    if isinstance(text_or_int, int):
        return int(str(text_or_int)[:-1])
    elif isinstance(text_or_int, str):
        return text_or_int[:-1]

def remove_first_character(text):
    if isinstance(text, str):
        text = text[1:]
    return text

def to_title(text):
    if isinstance(text, str):
        text = text.title()
    return text

def strip_text(text):
    if isinstance(text, str):
        text = text.strip()
    return text

def get_replace_function(to_replace_words: dict):
    def replace(value):
        nonlocal to_replace_words
        for k, v in to_replace_words.items():
            if isinstance(k, str) and isinstance(value, str):
                v = str(v)
                if k in value:
                    value = value.replace(k, v)
            else:
                if k == value:
                    value = v
        return value
    return replace

def convert_to_string(value):
    return str(value)
