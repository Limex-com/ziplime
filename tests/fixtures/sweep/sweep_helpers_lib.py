"""A module of the strategy's own code, imported by tests/fixtures/sweep/constructs.py."""


def _floor(value, low):
    if value < low:
        return low
    return value


def bounded_score(x, y):
    score = _floor(x, -1.0) * 2
    if y > 1:
        score = score + y
    return score
