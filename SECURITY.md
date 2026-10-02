# Security

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: **Security → Report a vulnerability** on
[Limex-com/ziplime](https://github.com/Limex-com/ziplime/security). Do not open a public issue for
a problem that is not fixed yet.

## A strategy is code, and ziplime runs it

A ziplime strategy is an ordinary Python file. `ziplime run`, `run_simulation()` and
`run_live_trading()` import it and execute it with your user's permissions
(`ziplime/core/algorithm_file.py`). That is the design, the same as for any Python program you run:
only run strategies you have read or trust.

### The local MCP server

`ziplime mcp` gives an AI agent the tools `write_strategy` and `run_backtest`. Together they let the
agent write Python to disk and execute it on your machine. A prompt injection — a web page, a
dataset card or a file the agent reads — can therefore become code running as you.

- `check_strategy_code` looks for mistakes (a missing `await`, a synchronous hook, wrong arity). It
  is **not** a sandbox and does not try to block malicious code.
- Read what the agent writes before you let it run, or run the server where a strategy can do no
  harm: a container or VM with no credentials and only the data directory mounted.
- The local server places no real orders. Live trading needs `run_live_trading()` and Lime
  brokerage credentials, which the MCP tools never receive.

The hosted server at ziplime.limex.com runs strategies on Limex infrastructure, not on your machine.

## Supported versions

Security fixes go into the latest release on PyPI.
