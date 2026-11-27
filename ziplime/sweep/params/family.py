"""Family for params."""

from typing import Literal
from pydantic import model_validator
from ziplime.sweep.params.support import Method
from ziplime.sweep.params.strict import Strict


class Family(Strict):
    """One family of synthetic paths: ``{"method": "stationary", "block": 20}``."""

    method: Method
    #: Shown in the report and the files; null -- the method and its settings
    name: str | None = None
    #: ``block``, ``circular``: bars per block; ``stationary``: the mean block length
    block: int = 20
    #: ``historical`` -- the paths drift as the real bars did; ``zero`` -- each instrument's mean
    #: log return is taken out, so a strategy can only earn from the bars' shape, not the trend
    drift: Literal["historical", "zero"] = "historical"
    #: ``student_t``: degrees of freedom of the shocks (more than 2)
    df: float = 4.0
    #: ``meboot``: trimming of the mean absolute change that sets the tails; widen each
    #: replicate whose spread came out narrower than the data's (R meboot's expand.sd)
    trim: float = 0.10
    expand_sd: bool = True

    @model_validator(mode="after")
    def check(self):
        if self.block < 1:
            raise ValueError("block has to be at least 1")
        if self.df <= 2:
            raise ValueError("df has to be more than 2")
        if not 0 <= self.trim < 0.5:
            raise ValueError("trim has to be in [0, 0.5)")
        return self

    @property
    def label(self) -> str:
        if self.name:
            return self.name
        settings = {"block": f"({self.block})", "circular": f"({self.block})",
                    "stationary": f"({self.block})", "student_t": f"(df={self.df:g})"}
        return self.method + settings.get(self.method, "") + \
            (", zero drift" if self.drift == "zero" else "")
