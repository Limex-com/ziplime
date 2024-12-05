import datetime
import logging
import multiprocessing
import sys
from dataclasses import asdict
from queue import Queue

import limexhub
import numpy
import pandas as pd
from click import progressbar
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.constants.fundamental_data import FundamentalData
from ziplime.data.abstract_fundamendal_data_provider import AbstractFundamentalDataProvider
from ziplime.domain.lime_quote import LimeQuote


class LimexHubFundamentalDataProvider(AbstractFundamentalDataProvider):
    def __init__(self, limex_api_key: str):
        self._limex_api_key = limex_api_key
        self._logger = logging.getLogger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)

    def get_fundamental_data(self,
                             symbols: list[str],
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             period: Period,
                             fundamental_data_list: set[str]
                             ):
        data = pd.DataFrame()
        for symbol in symbols:
            res = self._get_fundamental_data(symbol, date_from, date_to, period, fundamental_data_list)
            if res is None:
                logging.warning(f"No fundamental data for {symbol} for period {date_from}-{date_to}.")
                continue
            data = pd.concat([data, res], axis=0, ignore_index=False)
        data.set_index(["date", "symbol"], inplace=True)

        return data

    def _get_fundamental_data(self,
                              symbol: str,
                              date_from: datetime.datetime,
                              date_to: datetime.datetime,
                              period: Period,
                              fundamental_data_list: set[str]
                              ):

        # print("FUNDAMENTAL DATA")
        fundamental = self._limex_client.fundamental(
            symbol=symbol,
            from_date=date_from,
            to_date=date_to,
            fields=None
        )
        if fundamental.empty:
            return None
        fundamental["date"] = pd.to_datetime(fundamental.date, utc=True)
        dr = pd.date_range(date_from, date_to, freq='D')
        fundamental_new = pd.DataFrame(columns=["date", "symbol"])
        fundamental_new.set_index(keys=["date", "symbol"], inplace=True, drop=True)
        use_datetime = True
        for fund_col in FundamentalData:

            # print(fund_col.value)

            col_name = fund_col.value
            ttm_col = f"{col_name}_ttm"
            value_col = f"{col_name}_value"
            add_value_col = f"{col_name}_add_value"
            columns = ["date", "symbol"]
            if ttm_col in fundamental_data_list:
                columns.append(ttm_col)
            if value_col in fundamental_data_list:
                columns.append(value_col)
            if add_value_col in fundamental_data_list:
                columns.append(add_value_col)

            res_df = pd.DataFrame(columns=columns)
            if len(columns) <= 2:
                continue
            if "field" in fundamental.columns:
                values_for_col = fundamental[fundamental.field == col_name]
                for index, row in values_for_col.iterrows():
                    if use_datetime:
                        dt = datetime.datetime.combine(row.date, time=datetime.time(), tzinfo=datetime.timezone.utc)
                    else:
                        dt = row.date

                    vals = [dt, symbol]
                    if ttm_col in columns:
                        vals.append(row.ttm)
                    if value_col in columns:
                        vals.append(row.value)
                    if add_value_col in columns:
                        vals.append(row.add_value)
                    vals_df = pd.DataFrame([
                        vals,
                    ], columns=res_df.columns)
                    res_df = (res_df.copy() if vals_df.empty else vals_df if res_df.empty
                    else pd.concat([res_df, vals_df], ignore_index=True)
                              )
                    # res_df = pd.concat([pd.DataFrame([
                    #     vals,
                    # ], columns=res_df.columns), res_df], ignore_index=True)
            else:
                vals = [date_from, symbol]
                if ttm_col in columns:
                    vals.append(numpy.nan)
                if value_col in columns:
                    vals.append(numpy.nan)
                if add_value_col in columns:
                    vals.append(numpy.nan)

                res_df = pd.concat([pd.DataFrame([
                    vals,
                ], columns=res_df.columns), res_df], ignore_index=True)
            res_df.set_index("date", inplace=True, drop=True)
            res_df = res_df.reindex(dr, fill_value=None)
            res_df["symbol"] = symbol
            res_df.reset_index(inplace=True)
            res_df.rename(columns={"index": "date"}, inplace=True)
            res_df.set_index(["date", "symbol"], inplace=True)
            fundamental_new = pd.concat([res_df, fundamental_new], ignore_index=False, axis=1)

        fundamental_new.reset_index(inplace=True)
        filtered = fundamental_new[fundamental_new.symbol.notnull()]
        # filtered.set_index(["date", "symbol"], inplace=True)

        return filtered
