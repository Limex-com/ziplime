"""Table utils for huggingface_data_source."""

import polars as pl
from ziplime.data.data_sources.huggingface import hub
from ziplime.data.data_sources.huggingface.manifest import ManifestError, parse_manifest
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import _logger
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import is_address
from ziplime.data.data_sources.huggingface.huggingface_data_source.address_utils import parse_address


async def load_table(repo_id: str, config: str | None = None, revision: str | None = None,
                     row_filter: pl.Expr | None = None) -> pl.DataFrame:
    """Read one config of a Hub dataset as a plain frame, with no point-in-time treatment.

    Not everything in a dataset is a time series. Committee rosters, legislator biographies and
    filing ledgers have no instrument and often no usable date; they exist to be **joined** to the
    tables that do. Mounting them as a data source would be meaningless, so this returns the frame
    and leaves what to do with it to the caller.

    It is also the way to build a source the Hub does not publish directly: read two tables, join
    them, and hand the result to :meth:`HuggingFaceDataSource.from_frame`.

    Args:
        repo_id: ``owner/name``, or a full ``hf://owner/name/config`` address.
        config: Table to read. Defaults to the dataset's default.
        revision: Branch, tag or commit. Pinned to a commit, like every other read here.
        row_filter: Applied while the Parquet is read, not afterwards.

    Returns:
        The table, exactly as published.
    """
    repo, address_config = (parse_address(repo_id) if is_address(repo_id) else (repo_id, None))
    pinned = hub.resolve_revision(repo, revision=revision)
    manifest = parse_manifest(repo_id=repo, revision=pinned.sha,
                              manifest_json=hub.read_text(pinned, "manifest.json"),
                              readme=hub.read_text(pinned, "README.md"))
    spec = manifest.config(config or address_config)
    parts = [path for path in pinned.files if path.endswith(".parquet") and spec.matches(path)]
    if not parts:
        raise ManifestError(
            f"{repo}:{spec.name} declares {list(spec.paths)} but the repository holds no Parquet "
            f"file matching them at {pinned.short_sha}.")

    # `diagonal` because a config may be split across files whose columns differ -- the congress
    # trades table splits into house and senate, and the senate rows carry fewer fields.
    frames = [pl.read_parquet(hub.download(pinned, part)) for part in parts]
    frame = frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal")
    if row_filter is not None:
        frame = frame.filter(row_filter)
    _logger.info("Read a Hugging Face table", dataset=pinned.describe(), config=spec.name,
                 rows=len(frame), parts=len(parts))
    return frame
