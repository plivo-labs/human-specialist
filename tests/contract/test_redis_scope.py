"""Only hs.core builds the Redis client and only hs.hot sends commands on it, so every key and command lives in one
module. Other modules may import Redis's error classes."""

import ast
from pathlib import Path

import hs

SRC = Path(hs.__file__).parent


def reaches(path: Path) -> list[str]:
    """What `path` takes from Redis besides its error classes, and the client it reaches through hs.core."""
    found = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found += [a.name for a in node.names if a.name.split(".")[0] == "redis"]
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "redis":
            found += [f"{node.module}.{a.name}" for a in node.names if not a.name.endswith("Error")]
        elif isinstance(node, ast.ImportFrom) and node.module == "hs.core":
            found += ["hs.core.redis" for a in node.names if a.name == "redis"]
        elif isinstance(node, ast.Attribute) and node.attr == "redis" and getattr(node.value, "id", "") == "core":
            found.append("core.redis")
    return found


def test_only_core_builds_the_client_and_only_hot_reaches_it():
    allowed = {"core.py": ["redis.Redis", "redis.client.Pipeline"], "hot.py": ["hs.core.redis"]}
    assert {str(p.relative_to(SRC)): f for p in SRC.rglob("*.py") if (f := reaches(p))} == allowed
