import enum
import os

import structlog
from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
# l = structlog.get_logger(__name__)
# l.info("Initializing alembic")
# fileConfig(config.config_file_name)
# l.info("Done initializing alembic")
# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
# target_metadata = None

from ziplime.core.db.base import BaseModel

target_metadata = BaseModel.metadata
x_args = context.get_x_argument(as_dictionary=True)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def include_object(object, name, type_, reflected, compare_to):
    """
    Exclude views from Alembic's consideration.
    """

    return not object.info.get('is_view', False)


def get_url():
    print(x_args)
    return os.environ.get('SQLALCHEMY_DATABASE_URI', "sqlite:///:memory:")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    server = os.getenv("DB_SERVER", "db")
    db = os.getenv("DB_NAME", ":memory:")
    db_url_scheme_sync = os.getenv("DB_URL_SCHEME_SYNC", "sqlite")
    return f"{db_url_scheme_sync}://{user}:{password}@{server}/{db}"


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url, target_metadata=target_metadata, literal_binds=True, compare_type=True
    )

    with context.begin_transaction():
        context.run_migrations()


def my_compare_type(context, inspected_column,
                    metadata_column, inspected_type, metadata_type):
    # return False if the metadata_type is the same as the inspected_type
    # or None to allow the default implementation to compare these
    # types. a return value of True means the two types do not
    # match and should result in a type change operation.
    if type(metadata_column.type.python_type) == enum.EnumType and max(
            len(v) for v in metadata_column.type.enums) != inspected_column.type.length:
        return True
    return None


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section)
    if not configuration.get("sqlalchemy.url", ""):
        configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration, prefix="sqlalchemy.", poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, compare_type=my_compare_type, include_schemas=True,
            include_object=include_object
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
