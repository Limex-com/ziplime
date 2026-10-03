# Copilot instructions for Ziplime

## Project context

- This is a Python 3.12+ library for market data, asset repositories, trading simulations, and
  vectorized execution.
- Zipline was the gold standard of backtesting — until it was abandoned on pandas 0.x and Python 3.6. Dozens of forks patched it. None rebuilt it.
  Ziplime keeps the API you know and replaces everything underneath:
  - Polars instead of pandas/NumPy — Parquet-native, 2–5× faster, screaming on Apple Silicon
  - Full asyncio — the engine is async end to end
  - Any frequency — 1-minute, hourly, daily, weekly, monthly, or your own custom bars
  - OHLCV + point-in-time fundamentals — P/E, revenue, margins, earnings, as they were known at the time
  - Python 3.12+, modern packaging, active releases
- Use the existing domain model and repository layers. Keep persistence models, domain entities,
  services, and trading logic separate.
- Follow the package layout already used under `ziplime/`; place tests under the matching
  subsystem directory in `tests/` (`assets`, `core`, `data`, `finance`, `trading`, `vectorized`,
  and so on).

## Implementation rules

- Preserve existing public behavior and API compatibility unless a change is explicitly required.
  Prefer a small, local change over a broad rewrite.
- Use Python 3.12 features and precise type annotations. Prefer domain types, typed collections,
  `Self`, unions with `|`, and explicit `None` handling over untyped dictionaries and casts.
- Keep asynchronous boundaries asynchronous. Repository and database operations should use the
  existing `async`/`await` and SQLAlchemy async-session patterns; do not add blocking database work
  to services or simulation code.
- Keep database details inside repository implementations. Expose and consume domain entities at
  the repository boundary, with explicit conversion helpers between ORM models and entities.
- Use complete identity keys for lookups and state. When an object is scoped by multiple values
  (for example exchange, account, and asset), represent the composite key explicitly rather than
  nesting mutable dictionaries whose scope can be lost.
- Make missing-data behavior explicit. Preserve the repository's established distinction between
  raising a domain error and returning `None` when an opt-in fallback is requested.
- Preserve ordering when an API accepts an ordered collection of identifiers, and add a regression
  test when query implementation details could change that order.
- Remove dead imports, obsolete query helpers, and unused compatibility code when refactoring.
  Do not leave commented-out implementations or silent fallback behavior behind.
- Keep date and timezone handling explicit. Use the project's calendar utilities and timezone-aware
  values at simulation boundaries; do not compare naive and aware timestamps implicitly.
- Reuse existing utilities, entities, errors, caches, and fixtures before introducing new helpers.
  Raise the repository's domain-specific errors rather than generic exceptions where one exists.
- Unless classes are very short, keep them in separate files instead of keeping multiple of them in the same file
- Avoid writing both classes and functions in the same file. Keep functions in separate util files or inside the class itself

## Tests

- Add or update tests with every behavior change and refactor that changes a boundary.
- Organize tests by subsystem and use descriptive behavior-focused names such as
  `test_retrieve_all_preserves_requested_order`.
- Cover both the narrow behavior and the wiring around it. For repositories, test persistence
  round-trips, filtering by type/date, ordering, cache refreshes, and documented missing-data
  behavior. For simulation changes, include lifecycle or end-to-end coverage when the change
  crosses multiple components.
- Prefer small deterministic fixtures and temporary databases. Async repository tests must dispose
  their engines and clean up temporary resources.
- Keep tests independent and avoid relying on the checked-in database state when a test can create
  the required entities itself.
- Use the existing test styles consistently: pytest tests for focused behavior and
  `unittest.IsolatedAsyncioTestCase` where the repository test suite already uses it.

## Validation and style

- Run the smallest relevant test selection first, then broaden validation if the change affects
  shared repository, simulation, or trading infrastructure.
- Keep lines within the repository's 120-character limit and match surrounding formatting.
- Before finishing, check for unused imports, accidental API changes, incomplete async cleanup, and
  `git diff --check` failures.