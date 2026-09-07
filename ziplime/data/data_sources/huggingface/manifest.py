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
import datetime
import json
import re
from dataclasses import dataclass, field
from collections.abc import Iterable
from typing import Any

import structlog

_logger = structlog.get_logger(__name__)

#: Columns that may index a mounted dataset, best first. Each is a *knowledge* date: the moment
#: the row became observable. ``feature_available_at`` outranks ``knowledge_day`` because an
#: aggregate over a day is not readable until that day is over.
KNOWLEDGE_DATE_COLUMNS = (
    "feature_available_at",
    "knowledge_date",
    "knowledge_day",
    "available_at",
    "disclosure_date",
    "notification_date",
    "date",
)

#: Columns that describe when something *happened*. Never an index: the gap between these and the
#: knowledge date is exactly the look-ahead a point-in-time dataset exists to prevent.
EVENT_DATE_COLUMNS = (
    "event_date",
    "transaction_date",
    "period_of_report",
    "as_of_date",
    "held_as_of_date",
    "filing_date",
    "expiration_date",
)

#: Columns that say when a row's own event happened, best first -- used to **floor** the knowledge
#: date, never to replace it. A knowledge date earlier than its own event is impossible, and these
#: datasets contain such rows: congress ``trades`` has 499, one of them dated a thousand years
#: before the trade it describes by a misread year digit. Left alone, that row is visible from the
#: start of every backtest.
#:
#: Deliberately narrower than :data:`EVENT_DATE_COLUMNS`. ``expiration_date`` is an event in the
#: *future* -- when an option expires -- so flooring against it would hide every option trade until
#: after it expired. ``filing_date`` is excluded for the opposite reason: it is closer to a
#: knowledge date than an event one.
FLOOR_EVENT_COLUMNS = ("event_date", "transaction_date", "period_of_report", "as_of_date")

#: Columns naming the instrument a row is about, best first.
ENTITY_COLUMNS = ("ticker", "asset_ticker", "symbol", "entity_id")

#: ``entity_domain`` values this adapter knows how to resolve against the asset database.
KNOWN_ENTITY_DOMAINS = frozenset({"us_equities"})


class ManifestError(RuntimeError):
    """The dataset does not describe itself well enough to be mounted."""


class NoKnowledgeDateError(ManifestError):
    """The dataset offers no knowledge date, so it cannot be mounted point-in-time.

    Raised rather than falling back to an event date. A backtest that indexes congressional
    disclosures by transaction date is not slightly optimistic; it trades on information that was
    45 days away from being public.
    """


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


@dataclass(frozen=True)
class DatasetManifest:
    """What a dataset says about itself, from ``manifest.json`` and the README front matter."""

    repo_id: str
    revision: str
    name: str | None = None
    version: str | None = None
    data_type: str | None = None
    entity_domain: str | None = None
    configs: dict[str, ConfigSpec] = field(default_factory=dict)
    #: ``min``/``max`` knowledge date, when the dataset publishes them.
    coverage_start: datetime.date | None = None
    coverage_end: datetime.date | None = None
    #: The raw documents, for anything this class does not model.
    raw_manifest: dict[str, Any] = field(default_factory=dict)

    @property
    def default_config(self) -> str | None:
        """The config to mount when the caller names none."""
        if not self.configs:
            return None
        for preferred in ("features", "pit", "positions"):
            if preferred in self.configs:
                return preferred
        return next(iter(self.configs))

    def config(self, name: str | None) -> ConfigSpec:
        """Look up a config by name, or the default when ``name`` is ``None``."""
        wanted = name or self.default_config
        if wanted is None:
            raise ManifestError(
                f"{self.repo_id} declares no configs in manifest.json or its README front matter, "
                f"so there is no table to mount.")
        if wanted not in self.configs:
            raise ManifestError(
                f"{self.repo_id} has no config {wanted!r}. It publishes: "
                f"{', '.join(sorted(self.configs))}.")
        return self.configs[wanted]


def parse_manifest(repo_id: str, revision: str, manifest_json: str | None,
                   readme: str | None) -> DatasetManifest:
    """Build a :class:`DatasetManifest` from whichever of the two documents exist.

    Both are optional individually -- a dataset with only a README front matter is mountable, and
    so is one with only ``manifest.json`` -- but a dataset with neither cannot be, because there
    is no way to know which files hold which table.
    """
    manifest = _load_json(manifest_json, repo_id)
    front_matter = _load_front_matter(readme, repo_id)

    configs = _configs_from_readme(front_matter)
    # manifest.json wins where the two disagree: it is ziplime's own metadata, and it carries the
    # grain and the Delta path that the Hub's config block has no room for.
    configs.update(_configs_from_manifest(manifest))

    coverage = manifest.get("coverage") or {}
    return DatasetManifest(
        repo_id=repo_id,
        revision=revision,
        name=manifest.get("name") or front_matter.get("pretty_name"),
        version=manifest.get("version"),
        data_type=manifest.get("data_type"),
        entity_domain=manifest.get("entity_domain"),
        configs=configs,
        coverage_start=_as_date(coverage.get("min_knowledge_date")),
        coverage_end=_as_date(coverage.get("max_knowledge_date")),
        raw_manifest=manifest,
    )


def resolve_knowledge_column(columns: list[str], repo_id: str, config: str) -> str:
    """Pick the column to index on, or refuse.

    Args:
        columns: Column names present in the config's Parquet files.
        repo_id: For the error message.
        config: For the error message.

    Returns:
        The name of the knowledge-date column.

    Raises:
        NoKnowledgeDateError: The dataset carries no knowledge date. Event dates present in the
            data are named in the message, because they are what a caller will be tempted to
            reach for and exactly what must not be used.
    """
    present = set(columns)
    for candidate in KNOWLEDGE_DATE_COLUMNS:
        if candidate in present:
            return candidate

    events = [column for column in EVENT_DATE_COLUMNS if column in present]
    detail = (f" It does carry {', '.join(events)}, but those say when something happened rather "
              f"than when it became public; indexing on one would let a strategy trade on "
              f"information nobody had yet." if events else "")
    raise NoKnowledgeDateError(
        f"{repo_id}:{config} has no knowledge-date column -- none of "
        f"{', '.join(KNOWLEDGE_DATE_COLUMNS)} is present.{detail}")


def resolve_event_column(columns: list[str], declared: str | None = None) -> str | None:
    """The column to floor the knowledge date against, or ``None`` if there is none.

    Args:
        columns: Column names present in the config's Parquet files.
        declared: What the manifest names as its event date, if it names one. Trusted over the
            preference list, since the publisher knows which of several date columns is the event.

    Returns:
        The event column, or ``None`` when the dataset carries no usable one -- in which case the
        knowledge date stands on its own and nothing is floored.
    """
    present = set(columns)
    if declared and declared in present:
        return declared
    for candidate in FLOOR_EVENT_COLUMNS:
        if candidate in present:
            return candidate
    return None


def resolve_entity_column(columns: list[str], repo_id: str, config: str) -> str:
    """Pick the column naming the instrument each row is about."""
    present = set(columns)
    for candidate in ENTITY_COLUMNS:
        if candidate in present:
            return candidate
    raise ManifestError(
        f"{repo_id}:{config} has no instrument column -- none of {', '.join(ENTITY_COLUMNS)} is "
        f"present, so its rows cannot be matched to assets.")


def _configs_from_manifest(manifest: dict[str, Any]) -> dict[str, ConfigSpec]:
    """Read the ``configs`` block ziplime's own bundle metadata carries."""
    configs: dict[str, ConfigSpec] = {}
    for name, spec in (manifest.get("configs") or {}).items():
        if not isinstance(spec, dict):
            continue
        path = spec.get("path")
        if not path:
            continue
        configs[name] = ConfigSpec(name=name, paths=(path,), grain=spec.get("grain"),
                                   delta_path=spec.get("delta_path"))
    return configs


def _configs_from_readme(front_matter: dict[str, Any]) -> dict[str, ConfigSpec]:
    """Read the Hub's standard ``configs:`` front-matter block.

    Splits are folded together rather than kept apart: ziplime indexes by date and instrument, and
    a dataset that splits its rows by chamber or by year is still one series once mounted.
    """
    configs: dict[str, ConfigSpec] = {}
    for entry in front_matter.get("configs") or []:
        if not isinstance(entry, dict):
            continue
        name = entry.get("config_name")
        if not name:
            continue
        data_dir = entry.get("data_dir")
        paths: list[str] = []
        for data_file in entry.get("data_files") or []:
            path = data_file.get("path") if isinstance(data_file, dict) else data_file
            if not path:
                continue
            paths.append(f"{data_dir.rstrip('/')}/{path}" if data_dir else path)
        if paths:
            configs[name] = ConfigSpec(name=name, paths=tuple(paths))
    return configs


def _load_json(text: str | None, repo_id: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError as error:
        _logger.warning("Ignoring an unreadable manifest.json", repo_id=repo_id, error=str(error))
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _load_front_matter(readme: str | None, repo_id: str) -> dict[str, Any]:
    """Parse the YAML block a Hub README opens with, if it has one."""
    if not readme:
        return {}
    match = re.match(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", readme, re.DOTALL)
    if not match:
        return {}
    try:
        import yaml
    except ImportError:  # pragma: no cover - yaml ships with huggingface_hub
        _logger.warning("No yaml module; skipping README front matter", repo_id=repo_id)
        return {}
    try:
        loaded = yaml.safe_load(match.group(1))
    except Exception as error:
        _logger.warning("Ignoring unreadable README front matter", repo_id=repo_id,
                        error=str(error))
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _as_date(value: Any) -> datetime.date | None:
    """Read a date out of the ISO strings a manifest uses, tolerating a trailing ``Z``."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _glob_matches(pattern: str, path: str) -> bool:
    """Match a repo path against a ``*``/``**`` pattern.

    ``fnmatch`` is not usable here: its ``*`` crosses ``/``, so ``data/features/*.parquet`` would
    match a file nested three directories deeper and pull in tables the caller did not ask for.
    """
    regex = []
    index = 0
    while index < len(pattern):
        if pattern.startswith("**/", index):
            regex.append("(?:.*/)?")
            index += 3
        elif pattern.startswith("**", index):
            regex.append(".*")
            index += 2
        elif pattern[index] == "*":
            regex.append("[^/]*")
            index += 1
        elif pattern[index] == "?":
            regex.append("[^/]")
            index += 1
        else:
            regex.append(re.escape(pattern[index]))
            index += 1
    return re.fullmatch("".join(regex), path) is not None
