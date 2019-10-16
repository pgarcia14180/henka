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