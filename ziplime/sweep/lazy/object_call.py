"""Object call for lazy."""


class _ObjectCall:
    """``objects[position].name(*args)``: the function a record of a method call keeps."""

    def __init__(self, objects: list, name: str) -> None:
        self.objects, self.name = objects, name

    def __call__(self, position, *args, **kwargs):
        return getattr(self.objects[int(position)], self.name)(*args, **kwargs)

    def _identity(self):
        # The objects themselves come from initialize's results and stay; the list holding them
        # is made anew with every run of the batch
        return tuple(map(id, self.objects)), self.name

    def __eq__(self, other):
        return isinstance(other, _ObjectCall) and other._identity() == self._identity()

    def __hash__(self) -> int:
        return hash(self._identity())

    @property
    def __name__(self) -> str:
        return f"{type(self.objects[0]).__name__}.{self.name}"
