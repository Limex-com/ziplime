"""A point-in-time dataset on the Hugging Face Hub, mounted as a ziplime data source.

    df = await data.history(assets=[apple], bar_count=30,
                            data_source="hf://ZipLime/congress-trading/features")

No ingest step, no bundle to build: the first read resolves the dataset to a commit, downloads
only the Parquet parts the simulation window can reach, maps tickers onto the asset database and
keeps the result in memory. Every later read in the run is served from there.

What the mounted frame looks like
---------------------------------

Two columns are renamed so the engine's own filters apply unchanged, and everything else is
carried through as published:

``date``
    The dataset's **knowledge date** -- when the row became observable -- as a UTC timestamp.
    ziplime windows every source with ``date < simulation time``, so putting the knowledge date
    here is what makes the mount point-in-time by construction rather than by convention.
    :mod:`~ziplime.data.data_sources.huggingface.manifest` picks the column and refuses to
    substitute an event date for it.
``sid``
    The listing the row is about, resolved from the dataset's ticker column against the asset
    database.

Rows whose ticker resolves to nothing are dropped, and how many is reported: these datasets carry
bonds identified by CUSIP, delisted names and foreign listings that no equity database will match,
and a mount that silently kept 30% of its rows would be worse than one that says so.

Why the window matters
----------------------

The datasets are large -- insider-trading's ``features`` is 4.1 million rows across 144 Parquet
parts -- and partitioned by ``knowledge_year``. Passing the simulation's own window lets whole
partitions be skipped without being downloaded, so a 2012 backtest never fetches 2015. Without a
window everything is fetched, which is correct and slow.
"""

from ziplime.data.data_sources.huggingface.huggingface_data_source.resolution import (  # noqa: F401
    Resolution,
)
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import (  # noqa: F401
    ADDRESS_SCHEME,
)
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import (  # noqa: F401
    is_address,
)
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import (  # noqa: F401
    parse_address,
)
from ziplime.data.data_sources.huggingface.huggingface_data_source.table_utils import (  # noqa: F401
    load_table,
)
from ziplime.data.data_sources.huggingface.huggingface_data_source.hugging_face_data_source import (  # noqa: F401
    HuggingFaceDataSource,
)


__all__ = [
    'Resolution',
    'ADDRESS_SCHEME',
    'is_address',
    'parse_address',
    'load_table',
    'HuggingFaceDataSource',
]
