"""Support for frames."""


#: polars aggregation -> the window statistic it is (polars' std and var have ddof=1)
AGGREGATIONS = {"mean": "mean", "sum": "sum", "std": "std", "var": "var", "min": "min",
                "max": "max", "median": "median", "first": "first", "last": "last",
                "count": "count", "len": "count"}


HINT = ("read a frame of several instruments per instrument -- "
        "frame.filter(pl.col('sid') == asset.sid)['close'].mean(), or "
        "frame.group_by('sid').agg(pl.col('close').mean()) and its iter_rows()")
