"""The convention a Hugging Face dataset follows to be mountable by ziplime.

A dataset becomes usable here by describing itself, not by having code written for it. Two files
in the repository carry that description, and both are things a dataset publishes anyway:

* ``manifest.json`` -- ziplime's own bundle metadata (``name``, ``version``, ``data_type``,
  ``entity_domain``, per-config paths, coverage). The ZipLime datasets already publish it because
  it is what :mod:`ziplime.data.services.file_system_bundle_registry` writes.
* The README's YAML front matter -- the Hub's standard ``configs:`` block, which every dataset
  with more than one table has.

Either is enough to locate a config's Parquet files; between them they also give the coverage
window and the entity domain. Nothing here is specific to a particular publisher, which is the
point: a third party who publishes the same two files gets the same treatment.

Point-in-time is the part that has to be right
----------------------------------------------

Every dataset of this kind carries two dates, and they are not interchangeable:

* the **event date** -- when the trade happened, the quarter ended, the form was signed;
* the **knowledge date** -- when it became observable to someone watching the source.

A congressional trade surfaces up to 45 days after execution; a 13F reports a quarter that ended
45 days earlier and is amended for months afterwards -- the worst case in that dataset arrived 452
days late. Indexing on the event date hands a backtest information nobody had, and the resulting
equity curve looks wonderful for a reason that has nothing to do with the strategy.

So this module resolves the column ziplime indexes on -- the one every window filter compares
against -- to a knowledge date, and **refuses to use an event date** even when nothing else is
available. :data:`KNOWLEDGE_DATE_COLUMNS` is the preference order and
:data:`EVENT_DATE_COLUMNS` is the blacklist; a dataset offering only the latter raises rather
than quietly producing a look-ahead-biased run.

The order matters within the knowledge dates too. The insider-trading ``features`` config carries
both ``knowledge_day`` (the New York calendar day of the underlying events) and
``feature_available_at`` (midnight New York at the start of the *following* day, when the
aggregated row could first be read). Those differ by up to a day, always in the direction that
flatters a backtest, so the later one wins.
"""

from ziplime.data.data_sources.huggingface.manifest.support import (  # noqa: F401
    KNOWLEDGE_DATE_COLUMNS,
)
from ziplime.data.data_sources.huggingface.manifest.support import (  # noqa: F401
    EVENT_DATE_COLUMNS,
)
from ziplime.data.data_sources.huggingface.manifest.support import (  # noqa: F401
    FLOOR_EVENT_COLUMNS,
)
from ziplime.data.data_sources.huggingface.manifest.support import ENTITY_COLUMNS  # noqa: F401
from ziplime.data.data_sources.huggingface.manifest.support import (  # noqa: F401
    KNOWN_ENTITY_DOMAINS,
)
from ziplime.data.data_sources.huggingface.manifest.errors import ManifestError  # noqa: F401
from ziplime.data.data_sources.huggingface.manifest.errors import (  # noqa: F401
    NoKnowledgeDateError,
)
from ziplime.data.data_sources.huggingface.manifest.config_spec import ConfigSpec  # noqa: F401
from ziplime.data.data_sources.huggingface.manifest.dataset_manifest import (  # noqa: F401
    DatasetManifest,
)
from ziplime.data.data_sources.huggingface.manifest.utils import parse_manifest  # noqa: F401
from ziplime.data.data_sources.huggingface.manifest.utils import (  # noqa: F401
    resolve_knowledge_column,
)
from ziplime.data.data_sources.huggingface.manifest.utils import (  # noqa: F401
    resolve_event_column,
)
from ziplime.data.data_sources.huggingface.manifest.utils import (  # noqa: F401
    resolve_entity_column,
)


__all__ = [
    "KNOWLEDGE_DATE_COLUMNS",
    "EVENT_DATE_COLUMNS",
    "FLOOR_EVENT_COLUMNS",
    "ENTITY_COLUMNS",
    "KNOWN_ENTITY_DOMAINS",
    "ManifestError",
    "NoKnowledgeDateError",
    "ConfigSpec",
    "DatasetManifest",
    "parse_manifest",
    "resolve_knowledge_column",
    "resolve_event_column",
    "resolve_entity_column",
]
