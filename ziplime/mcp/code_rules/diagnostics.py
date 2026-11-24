"""Diagnostics for code_rules."""

from __future__ import annotations


def _problem(severity: str, rule: str, message: str, line: int | None = None) -> dict:
    entry = {"severity": severity, "rule": rule, "message": message}
    if line is not None:
        entry["line"] = line
    return entry
