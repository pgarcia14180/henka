from .dictionary import unpack_dictionary_values

def get_replace_function(to_replace_words: dict, case_insensitive = False):
    def replace(value):
        nonlocal to_replace_words
        for k, v in to_replace_words.items():
            if isinstance(k, str) and isinstance(value, str):
                v = str(v)
                if k in value:
                    if case_insensitive:
                        value = value.lower().replace(k.lower(), v)
                    else:
                        value = value.replace(k, v)
            else:
                if k == value:
                    value = v
        return value
    return replace

def filter_text(text: str, **kwargs):
    starts_with, ends_with, contains, and_condition, or_condition = unpack_dictionary_values(
        kwargs, 
        "starts_with", 
        "ends_with", 
        "contains", 
        "and_condition", 
        "or_condition")
    match = []
    if starts_with: match.append(text.startswith(starts_with))
    if ends_with: match.append(text.endswith(ends_with))
    if contains: match.append(contains in text)
    if and_condition or and_condition is None: return all(match)
    if or_condition: return any(match)
    return True