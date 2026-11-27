"""Config spec for manifest."""

from dataclasses import dataclass
from collections.abc import Iterable
from ziplime.data.data_sources.huggingface.manifest.matching_utils import _glob_matches


@dataclass(frozen=True)
class ConfigSpec:
    """One table inside a dataset: its name and where its Parquet files live."""

    name: str
    #: Repository-relative path patterns. May contain ``*`` and ``**``.
    paths: tuple[str, ...]
    grain: str | None = None
    #: Set when the config is published as a Delta table as well as plain Parquet.
    delta_path: str | None = None

    def matches(self, repo_file: str) -> bool:
        """Whether ``repo_file`` belongs to this config."""
        return any(_glob_matches(pattern, repo_file) for pattern in self.paths)

    def select(self, repo_files: Iterable[str]) -> tuple[str, ...]:
        """The Parquet files this config's rows live in -- **one copy of each row**.

        A config published as a Delta table as well as plain Parquet matches its declared glob
        twice over, and the two are the same rows in two layouts. ``ZipLime/insider-trading:pit``
        holds 344 Delta parts beside 341 partitioned ones; reading what the glob returns would
        report every Form 4 twice, silently and with no error anywhere.

        A Delta table's transaction log is Parquet too, and it is not data -- its columns are
        ``add``, ``remove``, ``metaData``. It sorts first, so it is also what a schema probe reads
        unless it is excluded here.

        The plain partitions win when a config has both, because that is what the config's own
        ``path`` and the README's ``data_files`` point at; the Delta copy is an extra artefact for
        engines that read Delta. A config that ships *only* a Delta table still resolves to it --
        ``ZipLime/congress-trading:pit`` is published that way -- and its parts are read directly
        without consulting the transaction log. That is correct only because these datasets declare
        themselves append-only: on a table that rewrote rows, superseded parts would still be on
        disk and would be read as live data.
        """
        matched = [path for path in repo_files
                   if self.matches(path) and "/_delta_log/" not in path]
        if not self.delta_path:
            return tuple(matched)
        prefix = self.delta_path.rstrip("/") + "/"
        outside = [path for path in matched if not path.startswith(prefix)]
        return tuple(outside or matched)
