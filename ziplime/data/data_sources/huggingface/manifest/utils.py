"""Utils for manifest."""

import datetime
import json
import re
from typing import Any
from ziplime.data.data_sources.huggingface.manifest.config_spec import ConfigSpec
from ziplime.data.data_sources.huggingface.manifest.dataset_manifest import DatasetManifest
from ziplime.data.data_sources.huggingface.manifest.support import ENTITY_COLUMNS
from ziplime.data.data_sources.huggingface.manifest.support import EVENT_DATE_COLUMNS
from ziplime.data.data_sources.huggingface.manifest.support import FLOOR_EVENT_COLUMNS
from ziplime.data.data_sources.huggingface.manifest.support import KNOWLEDGE_DATE_COLUMNS
from ziplime.data.data_sources.huggingface.manifest.errors import ManifestError
from ziplime.data.data_sources.huggingface.manifest.errors import NoKnowledgeDateError
from ziplime.data.data_sources.huggingface.manifest.support import _logger


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
