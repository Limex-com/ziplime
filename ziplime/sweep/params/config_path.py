"""Config path for params."""

from ziplime.sweep.params.range import Range
from ziplime.sweep.params.strict import Strict


class ConfigPath(Strict):
    """A parameter deeper than the config's first level::

        "target": {"path": "equities_to_trade.0.target_percentage", "values": [10, 20]}

    The key (``target``) is how the parameter is named in ``where`` and in the results."""

    path: str
    values: list[bool | int | float | str] | Range
