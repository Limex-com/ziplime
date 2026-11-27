"""Reading a dataset repository on the Hugging Face Hub, at a pinned revision.

Everything here is addressed by **commit**, never by branch. A dataset that enthusiasts keep
appending to is a moving target, and a backtest run against "whatever ``main`` was that afternoon"
cannot be reproduced or compared with one run a week later. So a branch name is resolved to a
commit once, up front, and every subsequent read -- the manifest, the file listing, each Parquet
part -- names that commit. The resolved value is reported so it can be recorded alongside the run
and passed back later to repeat it exactly.

``huggingface_hub`` does the transport: it caches by commit under the usual
``HF_HOME``/``~/.cache/huggingface`` layout, resumes interrupted downloads, and picks up
``HF_TOKEN`` for private or gated repositories. A file already in that cache is not fetched again,
which is what makes a second run of the same backtest start immediately.
"""

from ziplime.data.data_sources.huggingface.hub.support import FLOATING_REVISIONS  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.errors import HubUnavailable  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.errors import DatasetNotFound  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.repo_revision import RepoRevision  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.utils import resolve_revision  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.utils import download  # noqa: F401
from ziplime.data.data_sources.huggingface.hub.utils import read_text  # noqa: F401


__all__ = [
    "FLOATING_REVISIONS",
    "HubUnavailable",
    "DatasetNotFound",
    "RepoRevision",
    "resolve_revision",
    "download",
    "read_text",
]
