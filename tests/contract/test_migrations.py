"""The Alembic chain is the schema: after `upgrade head` on an empty database, autogenerate finds nothing to add."""

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_upgrade_head_matches_the_models(db_engine):
    # A fresh interpreter sees exactly the models alembic/env.py loads; DATABASE_URL is this worker's migrated copy.
    check = [sys.executable, "-m", "alembic", "check"]
    done = subprocess.run(check, cwd=REPO, env=os.environ, capture_output=True, text=True, check=False)
    assert done.returncode == 0, f"models and migrations differ; add a revision:\n{done.stdout}{done.stderr}"
