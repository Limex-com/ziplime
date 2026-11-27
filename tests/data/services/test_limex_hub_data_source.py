import datetime
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from ziplime.data.services.limex_hub_data_source import LimexHubDataSource
from ziplime.data.services.limex_history_tasks import fetch_historical_limex_data_task


def test_fetch_historical_limex_data_task():
    sample_candles = pd.DataFrame(
        {
            "open": [150.0],
            "high": [155.0],
            "low": [149.0],
            "close": [153.0],
            "volume": [1000.0],
        },
        index=pd.to_datetime(["2024-01-02 16:00:00"]),
    )
    sample_candles.index.name = "Date"

    with patch("limexhub.RestAPI") as mock_rest:
        client_instance = MagicMock()
        client_instance.candles.return_value = sample_candles
        mock_rest.return_value = client_instance

        date_from = datetime.datetime(2024, 1, 2, 0, 0, tzinfo=datetime.timezone.utc)
        date_to = datetime.datetime(2024, 1, 3, 0, 0, tzinfo=datetime.timezone.utc)

        df = fetch_historical_limex_data_task(
            date_from=date_from,
            date_to=date_to,
            limex_api_key="fake-key",
            symbol="AAPL",
            frequency=datetime.timedelta(days=1),
        )

        assert not df.is_empty()
        assert "price" in df.columns
        assert df["symbol"][0] == "AAPL"
        assert df["mic"][0] == "XNGS"


@pytest.mark.asyncio
async def test_limex_hub_data_source_get_data():
    source = LimexHubDataSource(limex_api_key="fake-key", maximum_threads=2)

    sample_df = pl.DataFrame({
        "date": [datetime.datetime(2024, 1, 2, 16, 0, tzinfo=datetime.timezone.utc)],
        "symbol": ["AAPL"],
        "mic": ["XNGS"],
        "price": [150.0],
        "o": [150.0],
        "h": [155.0],
        "l": [149.0],
        "c": [153.0],
        "v": [1000.0],
    })

    with patch(
        "ziplime.data.services.limex_hub_data_source.fetch_historical_limex_data_task",
        return_value=sample_df,
    ):
        date_from = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        date_to = datetime.datetime(2024, 1, 4, tzinfo=datetime.timezone.utc)
        res = await source.get_data(
            symbols=["AAPL"],
            frequency=datetime.timedelta(days=1),
            date_from=date_from,
            date_to=date_to,
        )
        assert len(res) == 1
        assert res["symbol"][0] == "AAPL"


def test_limex_hub_data_source_from_env():
    with patch.dict(os.environ, {"LIMEX_API_KEY": "env-key"}, clear=True):
        source = LimexHubDataSource.from_env()
        assert source._limex_api_key == "env-key"

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing LIMEX_API_KEY"):
            LimexHubDataSource.from_env()
