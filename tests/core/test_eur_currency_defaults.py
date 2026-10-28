from importlib import import_module

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations


TABLES = (
    ("futures_contracts", "margin_currency"),
    ("futures_root_symbols", "margin_currency"),
    ("bonds", "quote_currency"),
    ("bond_events", "currency"),
)


def test_currency_default_migration_preserves_existing_values_and_supports_rollback(tmp_path):
    revision = import_module("ziplime.alembic.versions.a9c7d3e41b62_eur_currency_defaults")
    historical = import_module("ziplime.alembic.versions.c1a4f7d2e9b3_settlement_and_margin_currency")
    previous_default = dict(historical._COLUMNS)["margin_currency"]
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'currencies.sqlite'}")
    metadata = sa.MetaData()
    tables = [sa.Table(name, metadata, sa.Column("id", sa.Integer, primary_key=True),
                       sa.Column(column, sa.String, nullable=False, server_default=previous_default))
              for name, column in TABLES]
    try:
        with engine.begin() as connection:
            metadata.create_all(connection)
            for table, (_, column) in zip(tables, TABLES):
                connection.execute(table.insert().values({"id": 1, column: "GBP"}))
            context = MigrationContext.configure(connection)
            with Operations.context(context):
                revision.upgrade()
                for table, (_, column) in zip(tables, TABLES):
                    connection.execute(table.insert().values(id=2))
                    assert connection.execute(sa.select(table.c[column]).where(table.c.id == 1)).scalar_one() == "GBP"
                    assert connection.execute(sa.select(table.c[column]).where(table.c.id == 2)).scalar_one() == "EUR"

                revision.downgrade()
                for table, (_, column) in zip(tables, TABLES):
                    connection.execute(table.insert().values(id=3))
                    assert connection.execute(sa.select(table.c[column]).where(table.c.id == 3)).scalar_one() == previous_default
                    assert connection.execute(sa.select(table.c[column]).where(table.c.id == 2)).scalar_one() == "EUR"

                revision.upgrade()
                for table, (_, column) in zip(tables, TABLES):
                    connection.execute(table.insert().values(id=4))
                    assert connection.execute(sa.select(table.c[column]).where(table.c.id == 4)).scalar_one() == "EUR"
    finally:
        engine.dispose()
