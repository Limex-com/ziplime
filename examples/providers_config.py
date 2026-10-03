"""Settings shared by the connector examples."""
from pathlib import Path

#: The asset database the examples read and write. Both connectors write into the same database --
#: instruments are keyed by (mic, symbol, asset type), so markets do not collide.
ASSET_DB_PATH = str(Path(Path(__file__).parent.parent, "data", "assets.sqlite").absolute())
