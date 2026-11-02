"""Alembic environment: DATABASE_URL from hs settings, target metadata from every model the app maps."""

from logging.config import fileConfig

from sqlalchemy import create_engine, pool

import hs.app  # noqa: F401  # registers every mapped model on DBBase.metadata
from alembic import context
from hs.infra.db import DBBase
from hs.infra.settings import settings

if context.config.config_file_name is not None:
    fileConfig(context.config.config_file_name)

with create_engine(settings.database_url, poolclass=pool.NullPool).connect() as connection:
    context.configure(connection=connection, target_metadata=DBBase.metadata)
    with context.begin_transaction():
        context.run_migrations()
