"""Repo revision for hub."""

import datetime
from dataclasses import dataclass
from ziplime.data.data_sources.huggingface.hub.support import FLOATING_REVISIONS


@dataclass(frozen=True)
class RepoRevision:
    """A dataset repository pinned to one commit."""

    repo_id: str
    #: The 40-character commit sha every read is addressed by.
    sha: str
    #: What the caller asked for -- a branch, a tag, a commit, or nothing.
    requested: str | None
    #: Repository-relative paths present at ``sha``.
    files: tuple[str, ...]
    last_modified: datetime.datetime | None = None

    @property
    def was_floating(self) -> bool:
        """Whether the caller named a moving target rather than a commit."""
        return self.requested in FLOATING_REVISIONS

    @property
    def short_sha(self) -> str:
        return self.sha[:8]

    def describe(self) -> str:
        """A one-line identity for logs and for the run's record."""
        asked = self.requested or "main"
        if self.was_floating:
            return f"{self.repo_id}@{self.short_sha} (resolved from {asked})"
        return f"{self.repo_id}@{self.short_sha}"
