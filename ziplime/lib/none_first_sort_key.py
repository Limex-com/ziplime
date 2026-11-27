"""String factorization sort keys."""


class _NoneFirstSortKey:
    """Box to sort ``None`` to the front of the categories list.
    """

    def __init__(self, value: str | bytes | None) -> None:
        self.value = value

    def __eq__(self, other: '_NoneFirstSortKey'):
        return self.value == other.value

    def __lt__(self, other: '_NoneFirstSortKey'):
        if self.value is None:
            return True
        if other.value is None:
            return False
        return self.value < other.value

    def __gt__(self, other: '_NoneFirstSortKey'):
        if self.value is None:
            return False
        if other.value is None:
            return True
        return self.value > other.value

    def __le__(self, other: '_NoneFirstSortKey'):
        return self < other or self == other

    def __ge__(self, other: '_NoneFirstSortKey'):
        return self > other or self == other
