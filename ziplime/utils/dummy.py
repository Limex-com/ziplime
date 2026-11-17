class DummyMapping:
    """
    Dummy object used to provide a mapping interface for singular values.
    """

    def __init__(self, value) -> None:
        self._value = value

    def __getitem__(self, key):
        return self._value
