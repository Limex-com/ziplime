"""Errors for hub."""


class HubUnavailable(RuntimeError):
    """``huggingface_hub`` is not installed, so nothing can be read from the Hub."""

    def __init__(self) -> None:
        super().__init__(
            "Reading datasets from the Hugging Face Hub needs the huggingface_hub package:\n"
            "    pip install \"ziplime[huggingface]\"\n"
            "It is an optional dependency of ziplime, so that installs which do not use the Hub "
            "do not carry it.")


class DatasetNotFound(RuntimeError):
    """The repository does not exist, or the token cannot see it."""
