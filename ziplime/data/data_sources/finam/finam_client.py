"""Async client for the Finam Trade API REST facade (https://api.finam.ru/docs/grpc/).

Finam exposes its gRPC services over REST at ``https://api.finam.ru/v1``. Only the read-only
market-data and reference endpoints ziplime needs for ingestion are implemented here.

Three details of the API drive the shape of this client:

* **Session tokens are short lived.** ``POST /v1/sessions`` exchanges the long-lived API secret
  for a session token that expires in roughly 15 minutes and goes into ``Authorization`` verbatim
  (no ``Bearer`` prefix). The client refreshes proactively and retries once on a 401.
* **Bars are range limited.** ``TIME_FRAME_D`` accepts about a year per request and
  ``TIME_FRAME_M1`` about a week; anything larger fails with
  ``INVALID_ARGUMENT: Invalid date range``. :meth:`FinamClient.bars` chunks transparently.
* **Bond calendars need widening and paging by hand.** ``/v1/bonds/past`` and ``/v1/bonds/future``
  require ``limit``, answer within about a year of today unless given date bounds, and never
  populate ``pagination.has_next``. :meth:`FinamClient.bond_events` handles all three.
* **Reference data needs an account.** ``GetAsset``/``GetAssetParams`` reject a token with no
  ``account_id`` (``Invalid arguments:account_id``) and an id the token does not own
  (``Account with id=... not found``). The client discovers the right id from ``TokenDetails``
  when one is not configured, and both methods return ``None`` rather than raising when no
  account is available at all, so callers can fall back to a local spec table.

Finam also rate-limits: a burst of bar requests starts answering ``429 Too Many Requests``, and
without backoff an ingest of a whole futures chain stalls. Requests retry on 429 with exponential
backoff, honouring ``Retry-After`` when the server sends it.

``Bars`` is also unreliable: the same request alternates between the correct series and an empty
``bars`` array, apparently depending on which backend replica serves it. Since an empty array is
also the legitimate answer for a contract that never traded, :meth:`FinamClient.bars` retries an
empty chunk a few times before believing it -- without that, ingestion silently loses months of
history and mis-dates contract expiries.

The client also insists on HTTP/2. Finam fronts the gRPC services with envoy, and over HTTP/1.1
envoy has to buffer a whole transcoded response to produce a ``Content-Length``; ``/v1/assets`` is
about 3.4 MB and overflows that buffer, answering ``500 Response not transcoded because the
transcoder's internal buffer size exceeds the configured limit``. Over HTTP/2 the response is
streamed in frames and the same request succeeds.
"""
import asyncio
import datetime
import os
from typing import Any, Self

import httpx
import structlog

FINAM_API_URL = "https://api.finam.ru"

#: ``interval`` size accepted by ``Bars`` per timeframe, measured against the live API. Kept a
#: little under the observed ceiling so that DST/timezone drift cannot push a chunk over the edge.
TIME_FRAME_MAX_RANGE: dict[str, datetime.timedelta] = {
    "TIME_FRAME_M1": datetime.timedelta(days=5),
    "TIME_FRAME_M5": datetime.timedelta(days=25),
    "TIME_FRAME_M15": datetime.timedelta(days=75),
    "TIME_FRAME_M30": datetime.timedelta(days=150),
    "TIME_FRAME_H1": datetime.timedelta(days=300),
    "TIME_FRAME_H2": datetime.timedelta(days=300),
    "TIME_FRAME_H4": datetime.timedelta(days=300),
    "TIME_FRAME_H8": datetime.timedelta(days=300),
    "TIME_FRAME_D": datetime.timedelta(days=300),
    "TIME_FRAME_W": datetime.timedelta(days=1800),
    "TIME_FRAME_MN": datetime.timedelta(days=3600),
    "TIME_FRAME_QR": datetime.timedelta(days=3600),
}

#: Bar frequency -> Finam timeframe enum value.
TIMEDELTA_TO_TIME_FRAME: dict[datetime.timedelta, str] = {
    datetime.timedelta(minutes=1): "TIME_FRAME_M1",
    datetime.timedelta(minutes=5): "TIME_FRAME_M5",
    datetime.timedelta(minutes=15): "TIME_FRAME_M15",
    datetime.timedelta(minutes=30): "TIME_FRAME_M30",
    datetime.timedelta(hours=1): "TIME_FRAME_H1",
    datetime.timedelta(hours=2): "TIME_FRAME_H2",
    datetime.timedelta(hours=4): "TIME_FRAME_H4",
    datetime.timedelta(hours=8): "TIME_FRAME_H8",
    datetime.timedelta(days=1): "TIME_FRAME_D",
    datetime.timedelta(days=7): "TIME_FRAME_W",
}


class FinamApiError(RuntimeError):
    """Raised when the Finam API answers with an error payload."""

    def __init__(self, code: int, message: str, url: str):
        super().__init__(f"Finam API error {code} for {url}: {message}")
        self.code = code
        self.message = message


def to_time_frame(frequency: datetime.timedelta) -> str:
    """Map a bar frequency onto a Finam ``TIME_FRAME_*`` value."""
    time_frame = TIMEDELTA_TO_TIME_FRAME.get(frequency)
    if time_frame is None:
        raise ValueError(
            f"Unsupported frequency for Finam: {frequency}. "
            f"Supported: {sorted(str(f) for f in TIMEDELTA_TO_TIME_FRAME)}"
        )
    return time_frame


def parse_decimal(value: dict[str, Any] | None) -> float | None:
    """Unwrap a Finam ``Decimal`` (``{"value": "83020.0"}``), which may use scientific notation."""
    if value is None:
        return None
    raw = value.get("value") if isinstance(value, dict) else value
    if raw is None or raw == "":
        return None
    return float(raw)


def parse_date(value: Any) -> datetime.date | None:
    """Parse a ``google.type.Date`` (``{"year": 2026, "month": 12, "day": 17}``).

    Preferred over the RFC-3339 ``future_details.expiration_date``, which is stamped at 21:00 UTC
    the previous day and turns into the wrong date if read as UTC.
    """
    if not value or not isinstance(value, dict) or not value.get("year"):
        return None
    return datetime.date(int(value["year"]), int(value["month"]), int(value["day"]))


def parse_timestamp(value: Any) -> datetime.datetime | None:
    """Parse a Finam timestamp, which is either RFC-3339 or a protobuf ``{"seconds": ...}`` pair."""
    if not value:
        return None
    if isinstance(value, dict):
        seconds = value.get("seconds")
        if seconds in (None, "", 0, "0"):
            return None
        return datetime.datetime.fromtimestamp(int(seconds), tz=datetime.timezone.utc)
    return datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class FinamClient:
    """Read-only client for Finam Trade API reference and market data.

    Args:
        secret: Long-lived API token (``tapi_sk_...``) issued in the Finam cabinet.
        account_id: Trading account id. Required by ``GetAsset``/``GetAssetParams``; without it
            those calls are skipped and return ``None``.
        max_concurrency: Ceiling on in-flight HTTP requests. Ingesting a whole futures chain issues
            hundreds of requests, and Finam starts returning 429 well before a browser would, so
            the default is deliberately low.
    """

    #: Session tokens live ~15 minutes; refresh early so a long ingest never trips over expiry.
    TOKEN_TTL = datetime.timedelta(minutes=10)

    #: How many times to re-ask for a bar chunk that came back empty. The failure alternates
    #: request-to-request, so a handful of attempts separated by a short pause is enough to tell a
    #: flaky replica apart from a contract that genuinely has no data.
    #: The failure alternates request-to-request, so the first retry goes out immediately (it lands
    #: on a different backend) and only the last one waits.
    EMPTY_BARS_RETRIES = 2
    EMPTY_BARS_RETRY_DELAY = 0.3

    #: Attempts, base delay and ceiling for a rate-limited (429) request. Ingesting a whole
    #: multi-root chain issues thousands of requests, and Finam throttles hard enough that a short
    #: backoff gives up while the limit is still in force.
    RATE_LIMIT_RETRIES = 8
    RATE_LIMIT_BASE_DELAY = 1.0
    RATE_LIMIT_MAX_DELAY = 30.0

    def __init__(self, secret: str, account_id: str | None = None,
                 base_url: str = FINAM_API_URL,
                 max_concurrency: int = 3,
                 timeout: float = 30.0,
                 logger=None):
        if not secret:
            raise ValueError("Finam API secret is required")
        self._secret = secret
        self.account_id = account_id
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._token: str | None = None
        self._resolved_account_id: str | None = None
        self._token_issued_at: datetime.datetime | None = None
        self._token_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._logger = logger or structlog.get_logger(__name__)

    @classmethod
    def from_env(cls, **kwargs) -> Self:
        """Build a client from ``FINAM_API_SECRET`` and the optional ``FINAM_ACCOUNT_ID``."""
        secret = os.environ.get("FINAM_API_SECRET")
        if not secret:
            raise ValueError("FINAM_API_SECRET environment variable is not set")
        return cls(secret=secret, account_id=os.environ.get("FINAM_ACCOUNT_ID"), **kwargs)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            # http2 is required, not an optimisation -- see the module docstring.
            self._client = httpx.AsyncClient(base_url=self._base_url, timeout=self._timeout,
                                             http2=True)
        return self._client

    async def _get_token(self, force_refresh: bool = False) -> str:
        async with self._token_lock:
            fresh = (
                self._token is not None
                and self._token_issued_at is not None
                and datetime.datetime.now(tz=datetime.timezone.utc) - self._token_issued_at < self.TOKEN_TTL
            )
            if fresh and not force_refresh:
                return self._token

            response = await self._http().post("/v1/sessions", json={"secret": self._secret})
            payload = response.json()
            if "token" not in payload:
                raise FinamApiError(payload.get("code", response.status_code),
                                    payload.get("message", response.text), "/v1/sessions")
            self._token = payload["token"]
            self._token_issued_at = datetime.datetime.now(tz=datetime.timezone.utc)
            self._logger.debug("Obtained Finam session token")
            return self._token

    async def _request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET ``path``, refreshing an expired token and backing off when rate limited."""
        token_refreshed = False
        for attempt in range(self.RATE_LIMIT_RETRIES + 1):
            async with self._semaphore:
                token = await self._get_token(force_refresh=token_refreshed)
                response = await self._http().get(path, params=params,
                                                  headers={"Authorization": token})

            if response.status_code == 429:
                if attempt == self.RATE_LIMIT_RETRIES:
                    raise FinamApiError(429, "Rate limited by Finam after "
                                             f"{self.RATE_LIMIT_RETRIES} retries", path)
                delay = min(float(response.headers.get("Retry-After") or
                                  self.RATE_LIMIT_BASE_DELAY * (2 ** attempt)),
                            self.RATE_LIMIT_MAX_DELAY)
                self._logger.debug("Rate limited, backing off", path=path, delay=delay,
                                   attempt=attempt + 1)
                await asyncio.sleep(delay)
                continue

            if response.status_code == 401 and not token_refreshed:
                token_refreshed = True
                continue

            try:
                payload = response.json()
            except ValueError:
                # Envoy answers some failures with plain text rather than JSON.
                raise FinamApiError(response.status_code, response.text[:200], path)
            if isinstance(payload, dict) and "code" in payload and "message" in payload:
                raise FinamApiError(payload["code"], payload["message"], path)
            response.raise_for_status()
            return payload
        raise FinamApiError(401, "Unauthorized after token refresh", path)

    async def token_details(self) -> dict[str, Any]:
        """Return session token information, including the accounts it may act on."""
        token = await self._get_token()
        async with self._semaphore:
            response = await self._http().post("/v1/sessions/details", json={"token": token})
            payload = response.json()
        if "code" in payload and "message" in payload:
            raise FinamApiError(payload["code"], payload["message"], "/v1/sessions/details")
        return payload

    async def get_account_id(self) -> str | None:
        """Return the account id to use for reference-data calls.

        A configured id wins; otherwise the first account the token owns is used. Reference data
        is read-only, so any account the token can see answers the same specifications.
        """
        if self.account_id:
            return self.account_id
        if self._resolved_account_id is None:
            try:
                account_ids = (await self.token_details()).get("account_ids") or []
            except FinamApiError as error:
                self._logger.warning("Could not read token details; contract specifications will "
                                     "come from the local table", error=str(error))
                account_ids = []
            self._resolved_account_id = account_ids[0] if account_ids else ""
            if self._resolved_account_id:
                self._logger.info("Using Finam account for reference data",
                                  account_id=self._resolved_account_id)
        return self._resolved_account_id or None

    async def exchanges(self) -> list[dict[str, Any]]:
        """List exchanges: ``[{"mic": "RTSX", "name": "MOSCOW EXCHANGE - DERIVATIVES MARKET"}, ...]``."""
        return (await self._request("/v1/exchanges")).get("exchanges", [])

    async def assets(self) -> list[dict[str, Any]]:
        """List currently listed instruments. Expired contracts are **not** included."""
        return (await self._request("/v1/assets")).get("assets", [])

    async def all_assets(self, only_active: bool = False,
                         only_disabled: bool = False) -> list[dict[str, Any]]:
        """List every instrument, archived ones included, following cursor pagination.

        The endpoint returns 3000 rows per page and a ``next_cursor``; without following it you get
        only the first page, which looks like a hard truncation. This is the authoritative source
        of expired futures contracts.

        Args:
            only_active: Return only instruments that are still listed.
            only_disabled: Return only archived instruments.
        """
        assets: list[dict[str, Any]] = []
        cursor = 0
        seen_cursors = set()
        while True:
            params: dict[str, Any] = {"cursor": cursor}
            if only_active:
                params["only_active"] = "true"
            if only_disabled:
                params["only_disabled"] = "true"
            payload = await self._request("/v1/assets/all", params=params)
            page = payload.get("assets") or []
            assets.extend(page)
            next_cursor = int(payload.get("next_cursor") or 0)
            # A zero or repeated cursor means the listing is done; the guard also stops a server
            # that keeps handing back the same page from looping forever.
            if not page or not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        self._logger.debug("Fetched all assets", count=len(assets), pages=len(seen_cursors) + 1)
        return assets

    #: ``/v1/bonds/*`` answers within about a year of today unless a date bound is given, so the
    #: full calendar has to be asked for explicitly. These are the widest bounds the API accepts.
    BOND_HISTORY_START = datetime.date(1990, 1, 1)
    BOND_HISTORY_END = datetime.date(2060, 1, 1)

    #: Events per ``/v1/bonds/*`` request. ``limit`` is **required**: omitted, the endpoint answers
    #: with no events and no pagination block at all rather than with a default page.
    BOND_EVENTS_PAGE_SIZE = 1000

    async def bonds_past(self, symbol: str,
                         date_from: datetime.date | None = None,
                         date_to: datetime.date | None = None,
                         page_size: int | None = None) -> list[dict[str, Any]]:
        """Realised calendar of one bond: coupons, amortizations and offers already paid.

        Defaults to the whole history rather than the endpoint's own window -- see
        :meth:`_bond_events`.
        """
        return await self._bond_events("past", symbol=symbol,
                                       date_from=date_from or self.BOND_HISTORY_START,
                                       date_to=date_to, page_size=page_size)

    async def bonds_future(self, symbol: str,
                           date_from: datetime.date | None = None,
                           date_to: datetime.date | None = None,
                           page_size: int | None = None) -> list[dict[str, Any]]:
        """Scheduled calendar of one bond: every coupon and amortization still to come.

        The final amortization is the redemption, so this is also where a live bond's maturity date
        comes from. Defaults to the whole remaining schedule.
        """
        return await self._bond_events("future", symbol=symbol, date_from=date_from,
                                       date_to=date_to or self.BOND_HISTORY_END,
                                       page_size=page_size)

    async def bond_events(self, symbol: str) -> list[dict[str, Any]]:
        """The complete calendar of one bond, realised and scheduled, oldest first.

        Both halves are needed: ``past`` stops at today and ``future`` starts there, and a backtest
        wants the coupons a bond has already paid *and* the redemption it has not reached yet.
        """
        past, future = await asyncio.gather(self.bonds_past(symbol), self.bonds_future(symbol))
        return past + future

    async def _bond_events(self, kind: str, symbol: str,
                           date_from: datetime.date | None,
                           date_to: datetime.date | None,
                           page_size: int | None = None) -> list[dict[str, Any]]:
        """Page through ``/v1/bonds/{kind}``.

        Three things about this endpoint drive the shape of this method:

        * **``limit`` is required.** Without it the response carries neither events nor a
          ``pagination`` block -- it looks like a bond with no calendar rather than a bad request.
        * **The default date window is about a year.** ``GET /v1/bonds/past`` for a bond issued in
          2021 returns its last two coupons; with ``date_from`` set to 1990 it returns all ten.
          Widening each side is therefore the default here, not an option.
        * **``pagination.has_next`` is never set.** It comes back ``false`` with ``total`` at 0
          even when a full page was returned, so paging is driven by the page being full rather
          than by what the server claims.

        ``sort_direction`` is accepted and ignored, so the result is sorted by the caller.
        """
        limit = page_size or self.BOND_EVENTS_PAGE_SIZE
        events: list[dict[str, Any]] = []
        offset = 0
        while True:
            params: dict[str, Any] = {"symbol": symbol, "limit": limit, "offset": offset}
            params.update(_date_params("date_from", date_from))
            params.update(_date_params("date_to", date_to))
            try:
                payload = await self._request(f"/v1/bonds/{kind}", params=params)
            except FinamApiError as error:
                # A listing with no bond calendar answers NOT_FOUND rather than an empty list.
                if error.code in (5, 404):
                    return events
                raise
            page = payload.get("events") or []
            events.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        return events

    async def get_asset(self, symbol: str) -> dict[str, Any] | None:
        """Fetch instrument details (``future_details.expiration_date``, ``contract_size``, ...).

        Returns ``None`` when no ``account_id`` is configured, since the endpoint rejects such
        tokens with ``Invalid arguments:account_id``.
        """
        account_id = await self.get_account_id()
        if not account_id:
            return None
        return await self._request(f"/v1/assets/{symbol}", params={"account_id": account_id})

    async def get_asset_params(self, symbol: str) -> dict[str, Any] | None:
        """Fetch trading parameters (margin requirements, tradability). ``None`` without an account."""
        account_id = await self.get_account_id()
        if not account_id:
            return None
        return await self._request(f"/v1/assets/{symbol}/params",
                                   params={"account_id": account_id})

    async def bars(self, symbol: str, time_frame: str,
                   start: datetime.datetime, end: datetime.datetime) -> list[dict[str, Any]]:
        """Fetch OHLCV bars for ``symbol``, chunking the request to fit Finam's range limit.

        Args:
            symbol: Instrument in ``ticker@mic`` form, e.g. ``SiZ6@RTSX``.
            time_frame: A ``TIME_FRAME_*`` value; see :func:`to_time_frame`.
            start: Inclusive start of the interval.
            end: Exclusive end of the interval.

        Returns:
            Bars sorted by timestamp and de-duplicated across chunk boundaries. Each bar keeps the
            raw Finam shape (``{"timestamp": ..., "open": {"value": ...}, ...}``).
        """
        max_range = TIME_FRAME_MAX_RANGE.get(time_frame, datetime.timedelta(days=300))
        windows = []
        window_start = start
        while window_start < end:
            window_end = min(window_start + max_range, end)
            windows.append((window_start, window_end))
            window_start = window_end

        chunks = await asyncio.gather(
            *(self._bars_chunk(symbol, time_frame, s, e) for s, e in windows)
        )

        by_timestamp = {bar["timestamp"]: bar for chunk in chunks for bar in chunk}
        return [by_timestamp[key] for key in sorted(by_timestamp)]

    async def _bars_chunk(self, symbol: str, time_frame: str,
                          start: datetime.datetime, end: datetime.datetime) -> list[dict[str, Any]]:
        params = {
            "timeframe": time_frame,
            "interval.start_time": _to_rfc3339(start),
            "interval.end_time": _to_rfc3339(end),
        }
        for attempt in range(self.EMPTY_BARS_RETRIES + 1):
            try:
                payload = await self._request(f"/v1/instruments/{symbol}/bars", params=params)
            except FinamApiError as error:
                # An unknown or never-listed contract answers with NOT_FOUND rather than empty bars.
                if error.code in (5, 404):
                    return []
                raise
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 404:
                    return []
                raise

            bars = payload.get("bars") or []
            if bars:
                if attempt:
                    self._logger.debug("Finam returned bars only after retry", symbol=symbol,
                                       time_frame=time_frame, attempt=attempt, bars=len(bars))
                return bars
            if attempt < self.EMPTY_BARS_RETRIES:
                if attempt:
                    await asyncio.sleep(self.EMPTY_BARS_RETRY_DELAY)
        return []

    async def has_bars(self, symbol: str, start: datetime.datetime, end: datetime.datetime) -> bool:
        """Cheaply test whether Finam has any daily history for ``symbol`` in the interval.

        Used to decide whether a generated historical contract ticker actually exists, since the
        reference endpoints cannot enumerate expired contracts.
        """
        max_range = TIME_FRAME_MAX_RANGE["TIME_FRAME_D"]
        window_start = start
        while window_start < end:
            window_end = min(window_start + max_range, end)
            if await self._bars_chunk(symbol, "TIME_FRAME_D", window_start, window_end):
                return True
            window_start = window_end
        return False


def _date_params(prefix: str, value: datetime.date | None) -> dict[str, int]:
    """Expand a date into the ``prefix.year``/``month``/``day`` triple the API expects."""
    if value is None:
        return {}
    return {f"{prefix}.year": value.year, f"{prefix}.month": value.month,
            f"{prefix}.day": value.day}


def _to_rfc3339(value: datetime.datetime) -> str:
    """Render a datetime as the UTC RFC-3339 string the API expects."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
