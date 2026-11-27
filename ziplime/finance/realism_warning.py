"""A reported limitation of a simulation."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class RealismWarning:
    """One effect the simulation does not reproduce."""

    code: str
    detail: str

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"
