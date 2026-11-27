"""Frame for api."""


class Frame:
    def __init__(self, columns: dict) -> None:
        self._columns = columns

    def __getitem__(self, field: str):
        try:
            return self._columns[field]
        except KeyError:
            raise KeyError(f"Field {field!r} was not asked for; asked for {list(self._columns)}.") from None
