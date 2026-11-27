"""Grpc chain error for grpc_chain."""

from __future__ import annotations


class GrpcChainError(RuntimeError):
    """The chain service answered in a way this source will not guess its way past."""
