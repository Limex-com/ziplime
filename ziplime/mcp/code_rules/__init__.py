"""An AST pass over a strategy, run before a backtest is spent on it.

The engine validates by executing, which means a broken hook is discovered
after the data is loaded and the clock has started. This is a syntax tree walk
that takes milliseconds and catches the mistakes that cost a run.

Two severities, and the split is the whole design:

- **blocking** — the engine is certain to refuse or the strategy provably
  cannot trade. Syntax errors, a hook that is not `async`, a hook with the
  wrong parameters.
- **advisory** — matched on *names*, so it can be wrong. A local variable
  called `order`, a `.loc` on something that is not a frame. **An advisory
  never blocks a run**: a false positive that stops a correct strategy is
  worse than the mistake it was looking for.
"""

from ziplime.mcp.code_rules.support import HOOKS  # noqa: F401
from ziplime.mcp.code_rules.support import REQUIRED_HOOKS  # noqa: F401
from ziplime.mcp.code_rules.support import AWAITED_CALLS  # noqa: F401
from ziplime.mcp.code_rules.support import PANDAS_ACCESSORS  # noqa: F401
from ziplime.mcp.code_rules.utils import check  # noqa: F401


__all__ = [
    "HOOKS",
    "REQUIRED_HOOKS",
    "AWAITED_CALLS",
    "PANDAS_ACCESSORS",
    "check",
]
