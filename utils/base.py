
"""
    Merge two dictionaries, excluding keys from the second dictionary.
    Returns a new dictionary with the contents of dict1 and dict2.
    If a key is in both dictionaries, the value from the second dictionary (dict2) is used.
"""
def merge_dicts_exclude_keys(dict1, dict2, exclude_keys):
    return {k: v for k, v in {**dict1, **dict2}.items() if k not in exclude_keys}

    
def check_for_string(row: list|dict, str_to_check: str) -> bool | None:
    if isinstance(row, list):
        return any(item == str_to_check for item in row)
    elif isinstance(row, dict):
        return row.get(str_to_check)
    return None
