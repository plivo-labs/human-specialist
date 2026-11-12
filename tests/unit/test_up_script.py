import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / "docker").mkdir()
    (tmp_path / "bin").mkdir()
    for name in ("up.sh", ".env.example"):
        shutil.copy(ROOT / "docker" / name, tmp_path / "docker" / name)
    for tool in ("docker", "curl"):
        stub = tmp_path / "bin" / tool
        stub.write_text(
            f'#!/bin/sh\necho "$*" >> "{tmp_path}/calls"\ncase "$*" in *exec*) test ! -f "{tmp_path}/offline";; esac\n'
        )
        stub.chmod(0o755)
    return tmp_path


def up(checkout, *args):
    env = {**os.environ, "PATH": f"{checkout / 'bin'}:{os.environ['PATH']}"}
    return subprocess.run(["bash", checkout / "docker/up.sh", *args], capture_output=True, text=True, env=env)


def values(checkout):
    lines = (checkout / "docker/.env").read_text().splitlines()
    return dict(line.split("=", 1) for line in lines if "=" in line and not line.startswith("#"))


def started(checkout):
    calls = checkout / "calls"
    return calls.exists() and "up -d --build --wait" in calls.read_text()


def test_the_first_run_writes_a_private_env_with_fresh_keys_and_a_rerun_keeps_them(checkout):
    assert up(checkout, "https://hs.test/").returncode == 0
    first = values(checkout)
    assert first["PUBLIC_BASE_URL"] == "https://hs.test"
    assert len(first["HS_OPERATOR_KEY"]) == len(first["HS_SECRET_KEY"]) == 44
    assert (checkout / "docker/.env").stat().st_mode & 0o777 == 0o600
    assert started(checkout)

    assert up(checkout).returncode == 0
    assert values(checkout) == first


def test_a_missing_url_or_a_short_key_stops_before_anything_starts(checkout):
    missing = up(checkout)
    assert missing.returncode == 1 and "docker/up.sh https://" in missing.stderr

    env = checkout / "docker/.env"
    env.write_text(env.read_text().replace("HS_SECRET_KEY=", "HS_SECRET_KEY=short"))
    short = up(checkout, "https://hs.test")
    assert short.returncode == 1 and "HS_SECRET_KEY is 5 bytes" in short.stderr
    assert not started(checkout)


def test_no_ui_leaves_out_the_ui_profile(checkout):
    assert up(checkout, "https://hs.test").returncode == 0
    assert "--profile ui" in (checkout / "calls").read_text()
    (checkout / "calls").unlink()
    done = up(checkout, "--no-ui", "https://hs.test")
    assert done.returncode == 0 and "--profile ui" not in (checkout / "calls").read_text()


def test_a_bad_url_argument_is_refused_and_never_saved(checkout):
    assert up(checkout, "https://hs.test").returncode == 0
    bad = up(checkout, "https:/")
    assert bad.returncode == 1 and "'https:/' is not a public URL" in bad.stderr
    assert values(checkout)["PUBLIC_BASE_URL"] == "https://hs.test"


def test_the_reapply_hint_compares_with_the_last_healthy_start(checkout):
    first = up(checkout, "https://hs.test")
    assert "Re-apply" not in first.stdout + first.stderr
    assert "Re-apply" not in up(checkout, "https://hs.test").stdout
    assert "Re-apply Plivo URLs" in up(checkout, "https://other.test").stdout


def test_a_service_that_cannot_reach_plivo_fails_loudly_and_says_what_to_do(checkout):
    assert up(checkout, "https://hs.test").returncode == 0
    assert "exec -T hs python" in (checkout / "calls").read_text()
    (checkout / "offline").touch()
    done = up(checkout, "https://hs.test")
    assert done.returncode == 1
    assert (
        "the service cannot reach Plivo at https://api.plivo.com: restart Docker, or check your network" in done.stderr
    )


def test_the_banner_prints_a_command_that_yields_only_the_key_and_commands_naming_the_project(checkout):
    plain = up(checkout, "https://hs.test").stdout
    assert "grep HS_OPERATOR_KEY docker/.env | cut -d= -f2-" in plain
    assert "Stop  docker compose -f docker/compose.yaml" in plain and " -p " not in plain
    (checkout / "docker/.env").write_text((checkout / "docker/.env").read_text() + "COMPOSE_PROJECT_NAME=copy\n")
    named = up(checkout, "https://hs.test").stdout
    assert "Stop  docker compose -p copy -f docker/compose.yaml --profile ui down" in named
