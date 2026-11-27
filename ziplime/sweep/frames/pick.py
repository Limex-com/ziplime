"""Pick for frames."""


class Pick:
    """``pl.col("sid") == x`` / ``.is_in([...])``: which instruments a filter keeps."""

    def __init__(self, values: list) -> None:
        self.values = values
