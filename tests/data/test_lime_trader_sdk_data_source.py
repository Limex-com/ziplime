import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import polars as pl
import pytest
from lime_trader.models.market import Period

from ziplime.data.services.lime_trader_sdk_data_source import LimeTraderSdkDataSource


@pytest.mark.asyncio
async def test_get_data_combines_quote_objects_and_filters_inclusive_date_bounds():
    start = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    end = start + datetime.timedelta(days=1)
    history = AsyncMock(side_effect=[
        [
            SimpleNamespace(timestamp=stamp, open=10.0, close=12.0, high=13.0, low=9.0, volume=100.0)
            for stamp in (start - datetime.timedelta(days=1), start, end + datetime.timedelta(days=1))
        ],
        [SimpleNamespace(timestamp=end, open=20.0, close=22.0, high=23.0, low=19.0, volume=200.0)],
    ])
    source = LimeTraderSdkDataSource.__new__(LimeTraderSdkDataSource)
    source._lime_sdk_client = Mock()
    source._lime_sdk_client.market.get_quotes_history = history

    result = await source.get_data(["AAA", "BBB"], datetime.timedelta(days=1), start, end)

    assert result["symbol"].to_list() == ["AAA", "BBB"]
    assert result["date"].to_list() == [start, end]
    assert result["open"].to_list() == [10.0, 20.0]
    assert result["close"].to_list() == [12.0, 22.0]
    assert result["price"].to_list() == result["close"].to_list()
    assert result["volume"].to_list() == [100.0, 200.0]
    assert result["exchange"].to_list() == ["LIME", "LIME"]
    assert result["exchange_country"].to_list() == ["US", "US"]
    history.assert_has_awaits([
        call(symbol=symbol, period=Period.DAY, from_date=start, to_date=end)
        for symbol in ("AAA", "BBB")
    ])


@pytest.mark.asyncio
@pytest.mark.parametrize("symbols", [[], ["EMPTY"]])
async def test_get_data_returns_typed_empty_frame_when_no_quotes_exist(symbols):
    start = datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    source = LimeTraderSdkDataSource.__new__(LimeTraderSdkDataSource)
    source._lime_sdk_client = Mock()
    source._lime_sdk_client.market.get_quotes_history = AsyncMock(return_value=[])

    result = await source.get_data(symbols, datetime.timedelta(days=1), start, start)

    assert result.is_empty()
    assert result.schema == {
        "open": pl.Float64, "close": pl.Float64, "price": pl.Float64,
        "high": pl.Float64, "low": pl.Float64, "volume": pl.Float64,
        "date": pl.Datetime(time_zone="UTC"), "exchange": pl.String,
        "symbol": pl.String, "exchange_country": pl.String,
    }
    assert source._lime_sdk_client.market.get_quotes_history.await_count == len(symbols)
