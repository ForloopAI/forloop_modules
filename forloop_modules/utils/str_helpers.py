import ast
import math
import re
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

# https://stackoverflow.com/a/59449963


def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])
    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)
    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def text_to_vector(text):
    text = text.replace("-", " ").replace("_", " ")
    word = re.compile(r'\w+')
    words = word.findall(text)
    return Counter(words)


def get_str_cosine_similarity(text1, text2):
    vector1 = text_to_vector(text1)
    vector2 = text_to_vector(text2)
    cosine_result = get_cosine(vector1, vector2)
    return cosine_result


def get_common_prefix(string1, string2):
    match = SequenceMatcher(None, string1, string2).find_longest_match(0, len(string1), 0, len(string2))

    print(match)  # -> Match(a=0, b=15, size=9)
    print(string1[match.a:match.a + match.size])  # -> apple pie
    print(string2[match.b:match.b + match.size])  # -> apple pie

    new_col_name = string1[match.a:match.a + match.size]

    return new_col_name


def parse_variable_value(value: str) -> Any:
    """Parse a string to a Python built-in data type, return original string if unsuccessful."""
    try:
        return ast.literal_eval(value)
    except Exception:
        return value
