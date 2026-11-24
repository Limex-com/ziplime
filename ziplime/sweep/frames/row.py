"""Row for frames."""

from ziplime.sweep.all_assets import ALL


class Row:
    """A row of ``iter_rows(named=True)``: ``row["sid"]`` is the instrument, every one at once."""

    def __init__(self, values: dict) -> None:
        self._values = values

    def __getitem__(self, key: str):
        if key == "sid":
            return ALL
        try:
            return self._values[key]
        except KeyError:
            raise KeyError(f"No column {key!r}; there are {['sid', *self._values]}.") from None

    def get(self, key: str, default=None):
        return self[key] if key == "sid" or key in self._values else default
