import datetime
from zoneinfo import ZoneInfo
import pytest
import asyncclick as click

from ziplime.utils.cli import Date, Timestamp
from ziplime.utils.cli.datetime_param import _DatetimeParam


def test_datetime_param_convert():
    param = _DatetimeParam()
    assert param.name == "_DATETIMEPARAM"
    res = param.convert("2024-01-01", None, None)
    assert res == datetime.datetime(2024, 1, 1)

    with pytest.raises(click.BadParameter, match="is not a valid _datetimeparam"):
        param.convert("invalid-date", None, None)


def test_timestamp_param():
    param = Timestamp(tz="UTC")
    res = param.convert("2024-01-01 12:00:00", None, None)
    assert res == datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

    # Passing existing datetime or date object
    dt_now = datetime.datetime(2024, 1, 1, 15, 0, 0)
    assert param.parser(dt_now) == dt_now.replace(tzinfo=ZoneInfo("UTC"))

    d = datetime.date(2024, 1, 1)
    assert param.parser(d) == datetime.datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC"))

    # Direct tzinfo object and astimezone conversion
    param_tzinfo = Timestamp(tz=datetime.timezone.utc)
    aware_dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    assert param_tzinfo.parser(aware_dt).tzinfo == datetime.timezone.utc

    custom_tz = object()
    assert Timestamp(tz=custom_tz).tz is custom_tz

    # Unsupported type raises bad parameter in convert
    with pytest.raises(click.BadParameter):
        param.convert(12345, None, None)


def test_date_param():
    param = Date()
    res = param.convert("2024-01-01", None, None)
    assert res == datetime.date(2024, 1, 1)

    param_ts = Date(as_timestamp=True)
    res_ts = param_ts.convert("2024-01-01 14:30:00", None, None)
    assert res_ts == datetime.datetime(2024, 1, 1, 0, 0, 0)
