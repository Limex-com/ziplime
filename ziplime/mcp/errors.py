"""Failures as a contract with the calling model, not as prose.

Every failure carries a stable ``code``, one line of what happened, one line of
what to do, and whether repeating the call is worth it. The model branches on
the code; the person reads the hint. Reword the prose freely, never rename a
code.
"""
from __future__ import annotations

# Codes a caller can act on. Anything not here is a bug in this server.
RETRYABLE = frozenset({"upstream_timeout", "data_source_unavailable"})


class ZiplimeMcpError(Exception):
    """One failure, rendered so an assistant can act on it without guessing."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "error",
        hint: str = "",
        detail: str = "",
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.hint = hint
        self.detail = detail

    def render(self) -> str:
        lines = [f"[{self.code}] {self.message}"]
        if self.hint:
            lines.append(f"What to do: {self.hint}")
        lines.append(
            "Retry: yes, the same call may work."
            if self.code in RETRYABLE
            else "Retry: no — the same call will fail again until something changes."
        )
        if self.detail:
            lines.append(f"Details: {self.detail}")
        return "\n".join(lines)


class NotFound(ZiplimeMcpError):
    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("code", "not_found")
        super().__init__(message, **kwargs)


class InvalidArguments(ZiplimeMcpError):
    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("code", "invalid_arguments")
        super().__init__(message, **kwargs)
