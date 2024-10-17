import datetime
import logging
import sys
import time
from dataclasses import asdict
from queue import Queue

import limexhub
import pandas as pd
from click import progressbar
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS, FundamentalData
from ziplime.domain.lime_quote import LimeQuote


class LimeDataProvider:
    def __init__(self, limex_api_key: str, lime_sdk_credentials_file: str):
        self._limex_api_key = limex_api_key
        self._lime_sdk_credentials_file = lime_sdk_credentials_file

        self._logger = logging.getLogger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    def get_fundamental_data(self, limex_client: limexhub.RestAPI,
                             symbol: str,
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             period: Period,
                             ):
        fundamental = limex_client.fundamental(
            symbol=symbol,
            from_date=date_from,
            to_date=date_to,
            fields=None
        )

        dr = pd.date_range(date_from, date_to, freq='D')
        fundamental_new = pd.DataFrame(columns=["date", "symbol"])
        fundamental_new.set_index(keys=["date", "symbol"], inplace=True, drop=True)
        if period in (Period.DAY, Period.MONTH, Period.WEEK, Period.QUARTER, Period.YEAR):
            use_datetime = False
        else:
            use_datetime = True
        use_datetime = True

        for fund_col in FundamentalData:
            col_name = fund_col.value

            values_for_col = fundamental[fundamental.field == col_name]
            # ttm_df = pd.DataFrame(columns=["date", "symbol", "name", "value"])
            # value_df = pd.DataFrame(columns=["date", "symbol", "name", "value"])
            # add_value_df = pd.DataFrame(columns=["date", "symbol", "name", "value"])
            res_df = pd.DataFrame(columns=["date", "symbol", f"{col_name}_ttm", f"{col_name}_value",
                                           f"{col_name}_add_value"
                                           ])
            for index, row in values_for_col.iterrows():
                if use_datetime:
                    dt = datetime.datetime.combine(row.date,time=datetime.time(), tzinfo=datetime.timezone.utc)
                else:
                    dt = row.date
                res_df = pd.concat([pd.DataFrame([
                    [dt, symbol, row.ttm, row.value, row.add_value],
                    # [dt, symbol, f"{row.field}_value", row.value],
                    # [dt, symbol, f"{row.field}_add_value", row.add_value],
                ], columns=res_df.columns), res_df], ignore_index=True)

                # ttm_df = pd.concat([pd.DataFrame([
                #     [dt, symbol, f"{row.field}_ttm", row.ttm],
                # ], columns=ttm_df.columns), ttm_df], ignore_index=True)
                #
                # value_df = pd.concat([pd.DataFrame([
                #     [dt, symbol, f"{row.field}_value", row.ttm],
                # ], columns=value_df.columns), value_df], ignore_index=True)
                #
                # add_value_df = pd.concat([pd.DataFrame([
                #     [dt, symbol, f"{row.field}_add_value", row.add_value],
                # ], columns=add_value_df.columns), add_value_df], ignore_index=True)



            # fundamental_new = pd.concat([pd.DataFrame([
            #     [dt, symbol, f"{row.field}_ttm", row.ttm],
            #     [dt, symbol, f"{row.field}_value", row.value],
            #     [dt, symbol, f"{row.field}_add_value", row.add_value]
            # ], columns=fundamental_new.columns), fundamental_new], ignore_index=True)
            res_df.set_index("date", inplace=True, drop=True)
            # value_df.set_index("date", inplace=True, drop=True)
            # add_value_df.set_index("date", inplace=True, drop=True)
            res_df = res_df.reindex(dr, fill_value=None).ffill().bfill()
            # value_df = value_df.reindex(dr, fill_value=None).ffill().bfill()
            # add_value_df = add_value_df.reindex(dr, fill_value=None).ffill().bfill()
            res_df.reset_index(inplace=True)
            res_df.rename(columns={"index": "date"}, inplace=True)
            res_df.set_index(["date", "symbol"], inplace=True)
            fundamental_new = pd.concat([res_df, fundamental_new], ignore_index=False, axis=1)

            # ttm_df.index = pd.DatetimeIndex(ttm_df.date)
        # value_df.index = pd.DatetimeIndex(value_df.date)
        # add_value_df.index = pd.DatetimeIndex(add_value_df.date)

        # fundamental_new.set_index(["date", "symbol", "name"], inplace=True, drop=True)
        # fundamental_new = fundamental_new.reindex(dr, fill_value=0)
        #     fundamental_new.sort_values(by=['date'], inplace=True)
        # fundamental_new.drop("date", axis="columns", inplace=True)
        fundamental_new.reset_index(inplace=True)
        filtered = fundamental_new[fundamental_new.symbol.notnull()]
        filtered.set_index(["date"], inplace=True)

        return filtered

    def fetch_historical_data_table(self, symbols: list[str],
                                    period: Period,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    show_progress: bool):

        def fetch_historical(limex_api_key: str, symbol: str):
            limex_client = limexhub.RestAPI(token=limex_api_key)
            timeframe = 3
            if period == Period.MINUTE:
                timeframe = 1
            elif period == Period.HOUR:
                timeframe = 2
            elif period == Period.DAY:
                timeframe = 3
            elif period == Period.WEEK:
                timeframe = 4
            elif period == Period.MONTH:
                timeframe = 5
            elif period == Period.QUARTER:
                timeframe = 6
            df = limex_client.candles(symbol=symbol,
                                      from_date=date_from,
                                      to_date=date_to,
                                      timeframe=timeframe)

            # fundamental = fundamental.reset_index()

            # fundamental = fundamental.set_index('date', drop=False)

            # date
            # total_share_holder_equity_value
            # total_share_holder_equity_ttm
            # total_liabilities_value
            # total_liabilities_ttm
            # total_assets_value
            # total_assets_ttm
            # shares_outstanding_value
            # shares_outstanding_ttm
            # roe_value
            # roe_ttm
            # revenue_value
            # revenue_ttm
            # return_on_tangible_equity_value
            # return_on_tangible_equity_ttm
            # quick_ratio_value
            # quick_ratio_ttm
            # price_sales_value
            # price_sales_ttm
            # price_fcf_value
            # price_fcf_ttm
            # fundamental = fundamental.set_index('date', drop=False)

            fundamental = self.get_fundamental_data(limex_client, symbol, date_from, date_to ,period=period)
            if len(df) > 0:
                df = df.reset_index()
                df = df.rename(
                    columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "Date": "date"})
                df["total_sells"] = 100
                # fundamental['date'] = pd.to_datetime(fundamental['date'])
                # df = pd.merge(df, fundamental, on='date')
                df = df.set_index('date', drop=False)
                df.index = pd.to_datetime(df.index, utc=True)
                # df["symbol"] = symbol
                df['dividend'] = 0
                df['split'] = 0
                final_df = pd.concat([df, fundamental], ignore_index=False, axis=1)
                final_df = final_df[final_df.date.notnull()]
                return final_df
            return df

        total_days = (date_to - date_from).days
        final = pd.DataFrame()

        if show_progress:
            with progressbar(length=len(symbols) * total_days, label="Downloading historical data from LimexHub",
                             file=sys.stdout) as pbar:
                res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
                    delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
                for item in res:
                    pbar.update(total_days)
                    final = pd.concat([final, item])
        else:
            res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
                delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                final = pd.concat([final, item])
        final = final.sort_index()
        return final

    def fetch_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool):
        historical_data = self.fetch_historical_data_table(symbols=symbols, period=period, date_from=date_from,
                                                           date_to=date_to,
                                                           show_progress=show_progress)

        yield historical_data

        # live_data_start_date = max(historical_data.index)[0].to_pydatetime().replace(
        #     tzinfo=datetime.timezone.utc) if len(historical_data) > 0 else date_from

        # for quotes in self.fetch_live_data_table(symbols=symbols, period=period, date_from=live_data_start_date,
        #                                          date_to=date_to,
        #                                          show_progress=show_progress):
        #     yield quotes

    def fetch_live_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool):

        live_data_queue = Queue()

        def fetch_live(lime_trader_sdk_credentials_file: str, symbol: str):
            lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
            try:
                quotes = lime_client.market.get_quotes_history(
                    symbol=symbol, period=period, from_date=date_from,
                    to_date=date_to
                )
                df = LimeDataProvider.load_data_table(
                    quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
                    show_progress=show_progress
                )
            except Exception as e:
                self._logger.error("Error fetching data using lime trader sdk")
                df = pd.DataFrame()
            live_data_queue.put(df)

        res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
            delayed(fetch_live)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)

        if show_progress:
            self._logger.info("Downloading live Lime Trader SDK metadata.")
        processed_symbols = 0
        while True:
            if processed_symbols == len(symbols):
                break
            item = live_data_queue.get()
            yield item
            processed_symbols += 1

    @staticmethod
    def load_data_table(quotes: list[LimeQuote], show_progress: bool = False):
        if not quotes:
            return pd.DataFrame()
        data_table = pd.DataFrame(
            [dict(**asdict(quote_hist.quote_history), symbol=quote_hist.symbol) for quote_hist in quotes], )

        data_table.rename(
            columns={
                "timestamp": "date",
                # "ex-dividend": "ex_dividend",
            },
            inplace=True,
            copy=False,
        )
        # data_table = data_table.reset_index()
        data_table = data_table.set_index('date', drop=False)
        return data_table
