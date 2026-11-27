"""Matching utils for manifest."""

import re


def _glob_matches(pattern: str, path: str) -> bool:
    """Match a repo path against a ``*``/``**`` pattern.

    ``fnmatch`` is not usable here: its ``*`` crosses ``/``, so ``data/features/*.parquet`` would
    match a file nested three directories deeper and pull in tables the caller did not ask for.
    """
    regex = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**/", index):
            regex.append("(?:.*/)?")
            index += 3
        elif pattern.startswith("**", index):
            regex.append(".*")
            index += 2
        elif pattern[index] == "*":
            regex.append("[^/]*")
            index += 1
        elif pattern[index] == "?":
            regex.append("[^/]")
            index += 1
        else:
            regex.append(re.escape(pattern[index]))
            index += 1
    return re.fullmatch("".join(regex), path) is not None
