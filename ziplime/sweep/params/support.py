"""Support for params."""

from pathlib import Path
from typing import Literal
from ziplime.sweep.params.config_path import ConfigPath
from ziplime.sweep.params.range import Range


#: Where bundles and the asset database live unless the sweep file says otherwise -- the same
#: defaults as the ``ziplime`` command line.
DEFAULT_STORAGE = Path(Path.home(), ".ziplime", "data")


DEFAULT_ASSET_DB = Path(Path.home(), ".ziplime", "assets.sqlite")


GridValue = list[bool | int | float | str] | Range | ConfigPath | bool | int | float | str


Method = Literal["iid", "shuffle", "block", "circular", "stationary", "meboot", "gbm", "student_t",
                 "garch"]
