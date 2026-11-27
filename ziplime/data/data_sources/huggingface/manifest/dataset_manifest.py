"""Dataset manifest for manifest."""

import datetime
from dataclasses import dataclass, field
from typing import Any
from ziplime.data.data_sources.huggingface.manifest.config_spec import ConfigSpec
from ziplime.data.data_sources.huggingface.manifest.errors import ManifestError


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
