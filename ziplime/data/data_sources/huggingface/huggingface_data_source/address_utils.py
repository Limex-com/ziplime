"""Address utils for huggingface_data_source."""

from typing import Any
from ziplime.data.data_sources.huggingface.huggingface_data_source.support import ADDRESS_SCHEME


def is_address(value: Any) -> bool:
    """Whether ``value`` is an ``hf://`` dataset address."""
    return isinstance(value, str) and value.startswith(ADDRESS_SCHEME)


def parse_address(address: str) -> tuple[str, str | None]:
    """Split ``hf://owner/name/config`` into its repository and config.

    The config is optional: ``hf://ZipLime/congress-trading`` mounts the dataset's default table.

    Raises:
        ValueError: The address names no repository.
    """
    if not is_address(address):
        raise ValueError(f"{address!r} is not a Hugging Face address; expected "
                         f"{ADDRESS_SCHEME}owner/name or {ADDRESS_SCHEME}owner/name/config.")
    parts = [part for part in address[len(ADDRESS_SCHEME):].split("/") if part]
    if len(parts) < 2:
        raise ValueError(f"{address!r} names no dataset; expected "
                         f"{ADDRESS_SCHEME}owner/name or {ADDRESS_SCHEME}owner/name/config.")
    repo_id = "/".join(parts[:2])
    config = "/".join(parts[2:]) or None
    return repo_id, config
