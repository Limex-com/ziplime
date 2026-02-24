from functools import reduce


def camel_case_to_snake_case(text: str) -> str:
    return reduce(lambda x, y: x + ('_' if y.isupper() else '') + y, text).lower()
