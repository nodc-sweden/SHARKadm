import re


def get_matching_strings(strings, match_strings):
    matching = dict()
    for item in match_strings:
        if item in strings:
            matching[item] = True
            continue
        for col in strings:
            if re.match(item, col):
                matching[col] = True
    return list(matching)