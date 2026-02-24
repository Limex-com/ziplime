from typing import Any

import pandas as pd

from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.domain.bar_data import BarData


def get_fundamental_data(
        bar_data: BarData,
        context: TradingAlgorithm,
        assets,
        fields,
        bar_count,
        frequency,
        fillna: Any = None
):
    if frequency != '1q':
        raise Exception("Currently only frequency of 1 quarter is supported for fundamental data")

    max_days_per_quarter = 92
    end_date = pd.to_datetime(bar_data.current_session)
    start_date = end_date - pd.Timedelta(days=max_days_per_quarter * bar_count)

    fundamental_data_bundle = context.fundamental_data_bundle
    first_date = fundamental_data_bundle.first_trading_day

    start_date_array = start_date
    if start_date < first_date:
        start_date_array = first_date

    dr = pd.date_range(start_date_array, bar_data.current_session, freq='D')

    res = fundamental_data_bundle.load_raw_arrays_full_range(
        [fields],
        start_date_array,
        bar_data.current_session,
        [assets],
        frequency='D'
    )[0]

    df = pd.DataFrame(data=res, index=dr)
    quarters = pd.date_range(start=start_date, end=end_date, freq='QE')
    df = df.reindex(quarters)
    if fillna is not None:
        df[df.isnull()] = fillna

    result_series = pd.Series(data=df[df.columns[0]], index=df.index)
    return result_series
