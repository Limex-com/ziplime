"""Opening the asset database on a machine that has never run ziplime, or ran an older release."""
import sqlite3

import pytest

from ziplime.core.ingest_data import get_asset_service
from ziplime.errors import IncompatibleAssetDatabase


def test_creates_missing_parent_directory(tmp_path):
    db_path = tmp_path / "fresh-home" / ".ziplime" / "assets.sqlite"

    get_asset_service(db_path=str(db_path))

    assert db_path.exists()


def test_database_from_a_1_19_release_is_reported_with_instructions(tmp_path):
    # 1.19 stamped its databases with root revision 8c43877dec20, which 2.10 no longer ships.
    db_path = tmp_path / "assets.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        connection.execute("INSERT INTO alembic_version VALUES ('8c43877dec20')")

    with pytest.raises(IncompatibleAssetDatabase) as error:
        get_asset_service(db_path=str(db_path))

    assert "8c43877dec20" in str(error.value)
    assert "ingest-assets --clear" in str(error.value)


def test_clear_rebuilds_a_database_from_a_1_19_release(tmp_path):
    db_path = tmp_path / "assets.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        connection.execute("INSERT INTO alembic_version VALUES ('8c43877dec20')")

    get_asset_service(db_path=str(db_path), clear_asset_db=True)

    with sqlite3.connect(db_path) as connection:
        (revision,) = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert revision != "8c43877dec20"
