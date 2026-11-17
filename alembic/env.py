import os
import sys
from logging.config import fileConfig

from sqlalchemy import create_engine, pool, text

from alembic import context

if context.config.config_file_name is not None:
    fileConfig(context.config.config_file_name)

# One transaction holds the lock, the schemas and every migration: instances that start together take turns, and a
# commit, a rollback or a lost connection releases the lock. Inside it, begin_transaction() begins nothing, and it
# writes BEGIN and COMMIT around an offline --sql script.
with create_engine(os.environ["DATABASE_URL"], poolclass=pool.NullPool).connect() as connection, connection.begin():
    connection.execute(text("SELECT pg_advisory_xact_lock(hashtext('hs.alembic'))"))
    schema = os.environ.get("HS_VERSION_SCHEMA", "hs")
    for name in {schema, "hs"}:
        # CREATE SCHEMA IF NOT EXISTS needs CREATE on the database even when the schema exists.
        if not connection.execute(text("SELECT 1 FROM pg_namespace WHERE nspname = :n"), {"n": name}).first():
            connection.execute(text(f"CREATE SCHEMA {connection.dialect.identifier_preparer.quote(name)}"))
    context.configure(connection=connection, version_table_schema=schema)
    held = text("SELECT count(*) FROM pg_tables WHERE schemaname = 'hs' AND tablename <> 'alembic_version'")
    if not context.get_context().get_current_revision() and connection.execute(held).scalar():
        sys.exit("schema hs holds tables but no recorded revision (docs/configuration.md, Sharing a database)")
    with context.begin_transaction():
        context.run_migrations()
