def get_has_content_function(content, opposite = False, case_insensitive = False):
    def has_content(value) -> bool:
        nonlocal content
        if not isinstance(value, str) or not isinstance(content, str):
            has_content_results = value == content
        elif isinstance(value, str) and isinstance(content, str):
            if case_insensitive:
                has_content_results = content.lower() in value.lower()
            else:        
                has_content_results = content in value
        if not opposite:
            return has_content_results
        else:
            return not has_content_results
    return has_content
