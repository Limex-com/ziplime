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
import datetime
from dataclasses import dataclass

import structlog

_logger = structlog.get_logger(__name__)

#: Branch names that mean "whatever is newest", as opposed to a commit to pin to.
FLOATING_REVISIONS = frozenset({"main", "master", "refs/heads/main", "refs/heads/master", None})


class HubUnavailable(RuntimeError):
    """``huggingface_hub`` is not installed, so nothing can be read from the Hub."""

    def __init__(self) -> None:
        super().__init__(
            "Reading datasets from the Hugging Face Hub needs the huggingface_hub package:\n"
            "    pip install huggingface_hub\n"
            "It is an optional dependency of ziplime, so that installs which do not use the Hub "
            "do not carry it.")


class DatasetNotFound(RuntimeError):
    """The repository does not exist, or the token cannot see it."""


def _api():
    """Return a Hub API client, with a clear error when the package is missing."""
    try:
        from huggingface_hub import HfApi
    except ImportError as error:
        raise HubUnavailable() from error
    return HfApi()


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


def resolve_revision(repo_id: str, revision: str | None = None) -> RepoRevision:
    """Pin ``repo_id`` to a commit and list the files it holds.

    Args:
        repo_id: ``owner/name`` on the Hub.
        revision: A branch, tag or commit. ``None`` means the default branch, which is resolved
            to the commit it currently points at.

    Returns:
        The pinned revision, including its file listing so callers need no second round trip.
    """
    api = _api()
    try:
        info = api.dataset_info(repo_id, revision=revision)
    except Exception as error:
        raise DatasetNotFound(
            f"Cannot read the dataset {repo_id!r} at revision {revision or 'main'!r} on the "
            f"Hugging Face Hub. If it is private or gated, set HF_TOKEN or run "
            f"`huggingface-cli login`. Underlying error: {error}") from error

    files = tuple(sorted(sibling.rfilename for sibling in (info.siblings or [])))
    pinned = RepoRevision(repo_id=repo_id, sha=info.sha, requested=revision, files=files,
                          last_modified=getattr(info, "last_modified", None))
    if pinned.was_floating:
        _logger.info("Pinned a Hugging Face dataset to the commit it is at now",
                     dataset=repo_id, revision=pinned.sha,
                     detail="Pass revision= to mount this exact data again later.")
    return pinned


def download(pinned: RepoRevision, repo_file: str) -> str:
    """Fetch one file at the pinned commit and return its local path.

    Cached by commit, so this is a no-op on a second run of the same backtest.
    """
    try:
        from huggingface_hub import hf_hub_download
    except ImportError as error:
        raise HubUnavailable() from error
    return hf_hub_download(repo_id=pinned.repo_id, filename=repo_file, repo_type="dataset",
                           revision=pinned.sha)


def read_text(pinned: RepoRevision, repo_file: str) -> str | None:
    """Read a small text file, or ``None`` when the repository does not carry it.

    Used for ``manifest.json`` and ``README.md``, either of which a dataset may legitimately lack.
    """
    if repo_file not in pinned.files:
        return None
    try:
        with open(download(pinned, repo_file), encoding="utf-8") as handle:
            return handle.read()
    except (OSError, UnicodeDecodeError) as error:
        _logger.warning("Could not read a dataset metadata file", dataset=pinned.repo_id,
                        file=repo_file, error=str(error))
        return None
