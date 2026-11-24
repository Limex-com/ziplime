"""Expired aware feed for grpc_chain."""

from __future__ import annotations
import datetime
from ziplime.data.data_sources.options.grpc_chain.support import _logger


class ExpiredAwareFeed:
    """A ``GrpcDataSource`` that can also fetch contracts which have already expired.

    The wrapped source resolves a symbol through ``GetSecurityInfo``, which answers for anything
    currently listed and raises for anything that is not. Its own fallback is to address the
    candle service by ticker and MIC -- and that service mostly does not accept one, so the
    request comes back empty. The effect is that an expired option looks like an option with no
    data, which is a very different thing and the reason this class exists.

    ``GetSecurityHistory`` does answer for them. Measured on 2026-09-13: three expired SPY series
    that ``GetSecurityInfo`` refused resolved through it and returned 405, 405 and 59 minute bars,
    going back to 2026-08-17. The data was always there; only the lookup was missing.

    **Why this seeds a private cache rather than overriding a method.** Delegating with
    ``__getattr__`` does not work here: ``get_data`` calls ``self.get_security_identifier``, and
    with delegation ``self`` is the *wrapped* object, so an override on the wrapper is never
    consulted -- the first version of this class was silently bypassed exactly that way, and the
    log said "falling back to ticker/mic" with none of the wrapper's own lines. Subclassing would
    bind correctly but needs the private package imported at class-definition time, which would
    make an optional dependency a hard one. So the id is resolved up front and written into the
    source's own per-symbol cache, which is the same slot its own lookup would have filled.
    """

    def __init__(self, data_source) -> None:
        self._source = data_source
        #: Resolved ids, so a chain of several hundred contracts costs one lookup each.
        self._resolved: dict[str, int] = {}

    def __getattr__(self, name: str):
        """Everything this does not override belongs to the wrapped source."""
        return getattr(self._source, name)

    async def _resolve_expired(self, channel, symbol: str) -> int | None:
        """The security id of a contract the ordinary lookup will not find."""
        from ziplime_grpc_data_source_private.grpc_stubs.grpc.reference import (
            securityreference_pb2 as reference,
            securityreference_pb2_grpc as reference_grpc,
        )
        from ziplime_grpc_data_source_private.grpc_stubs.proto.common import (
            securityidentifier_pb2 as identifiers,
        )

        ticker, _, mic = symbol.partition("@")
        identifier = identifiers.SecurityIdentifier()
        identifier.ticker_mic.ticker = ticker
        identifier.ticker_mic.mic = mic or "OPRA"
        stub = reference_grpc.SecurityReferenceStub(channel)
        token = await self._source.get_token()
        try:
            # Server-streaming, not unary: awaiting the call object rather than iterating it
            # fails with "object UnaryStreamCall can't be used in 'await' expression", which
            # reads like a protocol error and is really a missing `async for`.
            async for response in stub.GetSecurityHistory(
                    reference.SecurityHistoryRequest(identifier=identifier),
                    metadata=(("authorization", token),)):
                for security in response.securities:
                    found = security.security.security.common.security_id
                    if found:
                        return found
        except Exception as error:  # noqa: BLE001 - a miss here is "not found", not a failure
            _logger.debug("No history for this contract", symbol=symbol,
                          error=f"{type(error).__name__}: {error}")
        return None

    async def _prime(self, symbols: list[str]) -> None:
        """Make sure each symbol has a security id in the wrapped source's cache."""
        import asyncio as _asyncio

        import grpc

        unknown = [symbol for symbol in symbols if symbol not in self._resolved]
        if not unknown:
            return
        endpoint = self._source._server_url
        async with grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials()) as channel:
            for symbol in unknown:
                found = await self._resolve_expired(channel, symbol)
                if found is None:
                    continue
                self._resolved[symbol] = found
                _logger.debug("Resolved an expired contract through the history service",
                              symbol=symbol, security_id=found)

        loop = _asyncio.get_running_loop()
        for symbol, security_id in self._resolved.items():
            if symbol in self._source._security_id_tasks:
                continue
            settled = loop.create_future()
            settled.set_result(security_id)
            self._source._security_id_tasks[symbol] = settled

    async def get_data(
        self,
        symbols: list[str],
        frequency: datetime.timedelta,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        **kwargs,
    ):
        """The wrapped source's `get_data`, with expired contracts resolved first."""
        await self._prime(list(symbols))
        return await self._source.get_data(symbols=symbols, frequency=frequency,
                                           date_from=date_from, date_to=date_to, **kwargs)
