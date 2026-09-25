import datetime as dt
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from ziplime.assets.repositories.sqlalchemy_adjustments_repository import (
    SqlAlchemyAdjustmentRepository,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class SqlAlchemyAdjustmentRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_adjustments_accepts_timezone_aware_dates(self):
        with tempfile.TemporaryDirectory(prefix="ziplime-adjustments-") as temp_dir:
            db_path = Path(temp_dir) / "assets.sqlite"
            shutil.copy2(PROJECT_ROOT / "data" / "assets.sqlite", db_path)
            repository = SqlAlchemyAdjustmentRepository(
                db_url=f"sqlite+aiosqlite:///{db_path}"
            )

            result = await repository.load_adjustments(
                dates=pd.date_range(
                    dt.datetime(2024, 1, 2, tzinfo=dt.timezone.utc),
                    periods=2,
                    freq="D",
                ),
                assets=pd.Index([], dtype="int64"),
                should_include_splits=True,
                should_include_mergers=True,
                should_include_dividends=True,
                adjustment_type="all",
            )

            self.assertEqual(result, {"price": {}, "volume": {}})
            await repository.session_maker.kw["bind"].dispose()

    def test_json_round_trip_preserves_database_url(self):
        repository = SqlAlchemyAdjustmentRepository("sqlite+aiosqlite:///assets.sqlite")

        self.assertEqual(
            SqlAlchemyAdjustmentRepository.from_json(repository.to_json()).db_url,
            repository.db_url,
        )
