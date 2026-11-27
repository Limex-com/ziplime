"""Utils for hub."""

from ziplime.data.data_sources.huggingface.hub.errors import DatasetNotFound
from ziplime.data.data_sources.huggingface.hub.errors import HubUnavailable
from ziplime.data.data_sources.huggingface.hub.repo_revision import RepoRevision
from ziplime.data.data_sources.huggingface.hub.support import _logger


def _api():
    """Return a Hub API client, with a clear error when the package is missing."""
    try:
        from huggingface_hub import HfApi
    except ImportError as error:
        raise HubUnavailable() from error
    return HfApi()


_RESOLVED: dict[tuple[str, str | None], RepoRevision] = {}


def resolve_revision(repo_id: str, revision: str | None = None) -> RepoRevision:
    """Pin ``repo_id`` to a commit and list the files it holds.

    Args:
        repo_id: ``owner/name`` on the Hub.
        revision: A branch, tag or commit. ``None`` means the default branch, which is resolved
            to the commit it currently points at.

    Returns:
        The pinned revision, including its file listing so callers need no second round trip.
    """
    cached = _RESOLVED.get((repo_id, revision))
    if cached is not None:
        return cached

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
    _RESOLVED[(repo_id, revision)] = pinned
    # A commit resolves to itself, so record that too: a caller that pinned the sha explicitly
    # should not go to the network either.
    _RESOLVED[(repo_id, pinned.sha)] = pinned
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
