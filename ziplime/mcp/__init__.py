"""A local MCP server over the ziplime engine.

    python -m ziplime.mcp

Speaks MCP over stdio, so a client launches it as a subprocess. Nothing here
reaches a hosted service: strategies, bundles and results are files on this
machine.
"""

__all__ = ["main", "serve_stdio"]


def main() -> None:
    """Run the server, owning the event loop. Imported lazily so `-m ziplime.mcp` stays cheap."""
    from .server import main as _main

    _main()


async def serve_stdio() -> None:
    """Run the server inside a loop the caller already owns -- what `ziplime mcp` needs."""
    from .server import serve_stdio as _serve

    await _serve()
