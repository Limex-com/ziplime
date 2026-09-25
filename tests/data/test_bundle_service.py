import datetime

import polars as pl
import pytest

from ziplime.data.services.data_bundle_source import DataBundleSource


class _Source(DataBundleSource):
    async def get_data(self, symbols, frequency, date_from, date_to, **kwargs):
        return pl.DataFrame()


def test_data_bundle_source_requires_async_get_data_and_rejects_sync_calls():
    assert not DataBundleSource.__abstractmethods__.isdisjoint({"get_data"})
    with pytest.raises(TypeError):
        DataBundleSource()
    with pytest.raises(NotImplementedError):
        _Source().get_data_sync([], datetime.timedelta(days=1),
                                datetime.datetime(2024, 1, 1),
                                datetime.datetime(2024, 1, 2))
