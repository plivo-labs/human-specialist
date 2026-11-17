import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GCP = ROOT / "deploy/gcp"
STUB = Path(__file__).with_name("gcloud_stub.py")
AUTH_ID = "MAEXAMPLE0123456789A"
IMAGE = "us-central1-docker.pkg.dev/demo/cloud-run-source-deploy/human-specialist"
KEYS = ["human-specialist-operator-key", "human-specialist-secret-key"]

# The Cloud Run Button decodes app.json into these types with DisallowUnknownFields, so anything else fails the deploy
# (GoogleCloudPlatform/cloud-run-button, its app.json parser at cdfc83a).
HEROKU = {"description": str, "keywords": list, "logo": str, "repository": str, "website": str, "stack": str}
TOP = {"name": str, "env": dict, "options": dict, "build": dict, "hooks": dict, "formation": object, **HEROKU}
ENV = {"description": str, "value": str, "required": bool, "generator": str, "order": int}
OPTIONS = {"allow-unauthenticated": bool, "memory": str, "cpu": str, "port": int, "http2": bool, "concurrency": int}


def fits(value, types):
    return type(value) is dict and all(k in types and types[k] in (object, type(v)) for k, v in value.items())


@pytest.fixture
def cloud(tmp_path):
    (tmp_path / "gcloud").write_text(f'#!/bin/sh\nexec "{sys.executable}" -I "{STUB}" "$@"\n')
    (tmp_path / "gcloud").chmod(0o755)
    state(tmp_path, have=[], secrets={}, passwords={})
    return tmp_path


def state(cloud, **changes):
    path = cloud / "state.json"
    now = {**(json.loads(path.read_text()) if path.exists() else {}), **changes}
    path.write_text(json.dumps(now))
    return now


def precreate(cloud, ok=True, auth_id=AUTH_ID):
    (cloud / "calls").write_text("")
    env = {
        **os.environ,
        "PATH": f"{cloud}:{os.environ['PATH']}",
        "GCLOUD_STUB": str(cloud),
        "GOOGLE_CLOUD_PROJECT": "demo",
        "GOOGLE_CLOUD_REGION": "us-central1",
        "K_SERVICE": "human-specialist",
        "IMAGE_URL": IMAGE,
        "HS_SETUP_PLIVO_AUTH_ID": auth_id,
    }
    done = subprocess.run(["bash", GCP / "precreate.sh"], capture_output=True, text=True, env=env)
    assert (done.returncode == 0) is ok, done.stderr
    return [json.loads(line) for line in (cloud / "calls").read_text().splitlines()]


def creates(calls):
    return [c for c in calls if "create" in c]


def test_app_json_fits_the_types_the_button_decodes_and_asks_only_the_auth_id():
    app = json.loads((GCP / "app.json").read_text())
    assert fits(app, TOP) and fits(app["options"], OPTIONS) and all(fits(e, ENV) for e in app["env"].values())
    assert set(app["env"]) == {"HS_SETUP_PLIVO_AUTH_ID"} and "build" not in app
    # The button redeploys the service on the port in options, 8080 when it is missing.
    assert app["options"] == {"port": 6004}
    # A precreate hook's failure stops the button; a prebuild or postbuild hook's does not.
    assert app["hooks"] == {"precreate": {"commands": ["./precreate.sh"]}}
    assert os.access(GCP / "precreate.sh", os.X_OK)


def test_the_button_builds_the_release_in_pyproject():
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    assert (GCP / "Dockerfile").read_text() == f"FROM ghcr.io/plivo-labs/human-specialist:{version}\n"
    button = f"https://github.com/plivo-labs/human-specialist&revision=v{version}&dir=deploy/gcp)"
    assert all(button in (ROOT / doc).read_text() for doc in ("README.md", "docs/deploy.md"))


def test_the_script_parses():
    assert subprocess.run(["bash", "-n", GCP / "precreate.sh"]).returncode == 0


def test_a_first_run_makes_everything_then_deploys_one_instance_that_migrates_before_it_serves(cloud):
    calls = precreate(cloud)
    now = state(cloud)
    deploy = now["deployed"]
    for flag in ("--min-instances=1", "--max-instances=1", "--no-cpu-throttling", "--port=6004", "--timeout=3600"):
        assert flag in deploy
    assert "--network=default" in deploy and "--set-cloudsql-instances=demo:us-central1:human-specialist" in deploy
    flags = dict(a[2:].split("=", 1) for a in deploy if a.startswith("--") and "=" in a)
    # The button's later patches keep the command.
    assert flags["command"] == "/app/migrate-and-serve" and not [a for a in deploy if a.startswith("--args")]
    # Cloud Run resolves a secret in the environment once, at start: a pinned first version is the one HS ever reads.
    refs = dict(kv.split("=") for kv in flags["set-secrets"].split(","))
    assert refs == {
        "HS_OPERATOR_KEY": "human-specialist-operator-key:1",
        "HS_SECRET_KEY": "human-specialist-secret-key:1",
        "DATABASE_URL": "human-specialist-database-url:1",
        "REDIS_URL": "human-specialist-redis-url:1",
    }
    env = dict(kv.split("=", 1) for kv in flags["set-env-vars"].split(","))
    assert env["PUBLIC_BASE_URL"] == "https://human-specialist-123456789012.us-central1.run.app"
    assert env["HS_SETUP_PLIVO_AUTH_ID"] == AUTH_ID
    redis = next(c for c in calls if c[:3] == ["redis", "instances", "create"])
    assert "--redis-config=maxmemory-policy=noeviction" in redis and "--enable-auth" in redis
    secrets = now["secrets"]
    assert secrets["human-specialist-redis-url"] == "redis://:stub-auth-string@10.0.0.3:6379/0"
    password = now["passwords"]["human-specialist/hs"]
    assert secrets["human-specialist-database-url"] == (
        f"postgresql://hs:{password}@/hs?host=/cloudsql/demo:us-central1:human-specialist"
    )
    assert all(len(secrets[k]) >= 32 for k in KEYS)
    flagfile = next(a.removeprefix("--flags-file=") for c in calls for a in c if a.startswith("--flags-file="))
    assert not Path(flagfile).exists() and not any(password in " ".join(c) for c in calls)
    assert not any(secrets[k] in " ".join(c) for k in KEYS for c in calls)


def test_a_second_run_makes_nothing_and_keeps_every_secret(cloud):
    precreate(cloud)
    first = state(cloud)
    assert creates(precreate(cloud)) == []
    now = state(cloud)
    assert now["secrets"] == first["secrets"] and now["passwords"] == first["passwords"]


@pytest.mark.parametrize(
    "stop", ["iam service-accounts create", "redis instances create", "sql instances create", "sql users create"]
)
def test_a_run_that_stopped_is_finished_by_the_next_with_the_secrets_it_made(cloud, stop):
    state(cloud, fail={stop: "ERROR: stopped"})
    assert precreate(cloud, ok=False)[-1][:3] == stop.split()
    half = state(cloud, fail={})
    assert "deployed" not in half
    precreate(cloud)
    now = state(cloud)
    assert "deployed" in now and all(now["secrets"][k] == v for k, v in half["secrets"].items())
    password = now["passwords"]["human-specialist/hs"]
    assert now["secrets"]["human-specialist-database-url"].startswith(f"postgresql://hs:{password}@")


def test_a_secret_left_without_a_version_gets_its_first_one(cloud):
    state(cloud, have=["secrets human-specialist-secret-key"])
    precreate(cloud)
    assert len(state(cloud)["secrets"]["human-specialist-secret-key"]) >= 32


@pytest.mark.parametrize(("key", "value"), [("redis", "CREATING"), ("host", ""), ("auth", "")])
def test_a_redis_instance_not_ready_stops_the_run_before_its_secret_is_made(cloud, key, value):
    state(cloud, have=["redis instances human-specialist"], **{key: value})
    calls = precreate(cloud, ok=False)
    assert not [c for c in calls if c[:2] == ["secrets", "create"]] and "deployed" not in state(cloud)


def test_a_run_without_the_auth_id_stops_before_any_call(cloud):
    assert precreate(cloud, ok=False, auth_id="") == []
