"""Data provider lookup and credential errors."""


class UnknownDataProvider(LookupError):
    """Raised when a provider name is not registered."""

    def __init__(self, name: str, known: list[str]) -> None:
        super().__init__(
            f"Unknown data provider {name!r}. Registered providers: {known or 'none'}. "
            f"A provider is missing if its package was removed or its dependencies are not installed."
        )
        self.name = name
        self.known = known


class MissingProviderCredentials(RuntimeError):
    """Raised when a provider is asked for but its credentials are not configured."""

    def __init__(self, provider: str, missing: tuple[str, ...]):
        super().__init__(
            f"Data provider {provider!r} needs these environment variables: {', '.join(missing)}."
        )
        self.provider = provider
        self.missing = missing
