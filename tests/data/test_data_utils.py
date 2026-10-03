import datetime

import polars as pl
import pytest
from polars.exceptions import ColumnNotFoundError

from ziplime.utils.data_utils import _backfill_symbol_data, backfill_sid_data


class _Asset:
    def __init__(self, sid: int, symbol: str | None = None):
        self.sid = sid
        self.symbol = symbol


class _Assets:
    async def get_assets_by_ids(self, ids):
        return [_Asset(sid) for sid in ids]

    async def get_exchange_equities_by_symbols(self, symbols):
        return [_Asset(index + 1, symbol.symbol) for index, symbol in enumerate(symbols)]


@pytest.mark.asyncio
async def test_backfill_symbol_data_adds_missing_dates_without_filling_values():
    data = pl.DataFrame(
        {
            "sid": [7, 7],
            "date": [
                datetime.datetime(2024, 1, 2),
                datetime.datetime(2024, 1, 4),
            ],
            "value": [10.0, 12.0],
        }
    )
    required = pl.DataFrame(
        {
            "date": [
                datetime.datetime(2024, 1, 2),
                datetime.datetime(2024, 1, 3),
                datetime.datetime(2024, 1, 4),
            ]
        }
    )

    result = await _backfill_symbol_data(data, _Assets(), required)

    assert result["date"].to_list() == required["date"].to_list()
    assert result["sid"].to_list() == [7, 7, 7]
    assert result.filter(pl.col("date") == datetime.datetime(2024, 1, 3))["value"].item() is None


@pytest.mark.asyncio
async def test_backfill_symbol_data_rejects_unknown_sid():
    class NoAssets(_Assets):
        async def get_assets_by_ids(self, ids):
            return []

    data = pl.DataFrame({"sid": [99], "date": [datetime.datetime(2024, 1, 2)]})

    with pytest.raises(ValueError, match="SIDs are missing"):
        await _backfill_symbol_data(data, NoAssets(), data.select("date"))


@pytest.mark.asyncio
async def test_backfill_sid_data_adds_sid_and_missing_dates_for_each_symbol():
    data = pl.DataFrame(
        {
            "symbol": ["AAA", "AAA", "BBB"],
            "date": [
                datetime.datetime(2024, 1, 2),
                datetime.datetime(2024, 1, 4),
                datetime.datetime(2024, 1, 3),
            ],
            "value": [10.0, 12.0, 20.0],
        }
    )
    required = pl.DataFrame(
        {
            "date": [
                datetime.datetime(2024, 1, 2),
                datetime.datetime(2024, 1, 3),
                datetime.datetime(2024, 1, 4),
            ]
        }
    )

    result = await backfill_sid_data(data, _Assets(), required)

    assert result.columns == ["symbol", "date", "value", "sid"]
    assert result.select("sid").unique().sort("sid")["sid"].to_list() == [1, 2]
    assert result.filter(
        (pl.col("symbol") == "AAA") & (pl.col("date") == datetime.datetime(2024, 1, 3))
    )["value"].item() is None
    assert result.filter(
        (pl.col("symbol") == "BBB") & (pl.col("date") == datetime.datetime(2024, 1, 2))
    )["value"].item() is None
    assert result.filter(
        (pl.col("symbol") == "AAA") & (pl.col("date") == datetime.datetime(2024, 1, 2))
    )["value"].item() == 10.0


@pytest.mark.asyncio
async def test_backfill_sid_data_rejects_unknown_symbol():
    class NoSymbols(_Assets):
        async def get_exchange_equities_by_symbols(self, symbols):
            return []

    data = pl.DataFrame(
        {
            "symbol": ["UNKNOWN"],
            "date": [datetime.datetime(2024, 1, 2)],
            "value": [1.0],
        }
    )

    with pytest.raises(ValueError, match="Symbols are missing"):
        await backfill_sid_data(data, NoSymbols(), data.select("date"))


@pytest.mark.asyncio
async def test_backfill_functions_require_their_identifier_and_date_columns():
    required = pl.DataFrame({"date": [datetime.datetime(2024, 1, 2)]})

    with pytest.raises(ValueError, match="sid.*date"):
        await _backfill_symbol_data(
            pl.DataFrame({"symbol": ["AAA"], "date": required["date"]}),
            _Assets(),
            required,
        )

    with pytest.raises((ColumnNotFoundError, KeyError, TypeError)):
        await backfill_sid_data(
            pl.DataFrame({"sid": [1], "date": required["date"]}),
            _Assets(),
            required,
        )
