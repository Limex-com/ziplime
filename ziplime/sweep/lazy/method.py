"""Method for lazy."""


class _Method:
    """``obj.name(...)``, as a function of the object: part of a record's key."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, owner, *args, **kwargs):
        return getattr(owner, self.name)(*args, **kwargs)

    def __eq__(self, other):
        return isinstance(other, _Method) and other.name == self.name

    def __hash__(self) -> int:
        return hash(("method", self.name))
