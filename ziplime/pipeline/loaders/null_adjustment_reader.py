"""An adjustment reader for synthetic data."""

import numpy as np
from pandas import DataFrame
from sqlite3 import connect as sqlite3_connect
from ziplime.assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository


class NullAdjustmentReader(SqlAlchemyAdjustmentRepository):
    """A SQLiteAdjustmentReader that stores no adjustments and uses in-memory
    SQLite.
    """

    def __init__(self) -> None:
        conn = sqlite3_connect(":memory:")
        writer = SqlAlchemyAdjustmentRepository(conn, None, None)
        empty = DataFrame(
            {
                "sid": np.array([], dtype=np.uint32),
                "effective_date": np.array([], dtype=np.uint32),
                "ratio": np.array([], dtype=float),
            }
        )
        empty_dividends = DataFrame(
            {
                "sid": np.array([], dtype=np.uint32),
                "amount": np.array([], dtype=np.float64),
                "record_date": np.array([], dtype="datetime64[ns]"),
                "ex_date": np.array([], dtype="datetime64[ns]"),
                "declared_date": np.array([], dtype="datetime64[ns]"),
                "pay_date": np.array([], dtype="datetime64[ns]"),
            }
        )
        writer.write(splits=empty, mergers=empty, dividends=empty_dividends)
        super(NullAdjustmentReader, self).__init__(conn)
