"""Shared SQLAlchemy statement counter for the query-count tests.

`count_statements(session)` records every SQL statement executed on the session's
connection while the block is active (via the ``before_cursor_execute`` event), so a
test can assert how many round-trips a code path makes — the primary check that an N+1
was collapsed or a ``COUNT(*)`` replaced a full row materialisation.

Leading-underscore module name → pytest does not collect it as a test file.
"""

from contextlib import contextmanager

from sqlalchemy import event


@contextmanager
def count_statements(session):
    """Yield a list that accumulates every SQL statement run on ``session``'s connection."""
    conn = session.connection()
    statements = []

    def _hook(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(conn, "before_cursor_execute", _hook)
    try:
        yield statements
    finally:
        event.remove(conn, "before_cursor_execute", _hook)
