import json
import re
import ssl
import subprocess
from pathlib import Path

import pytest
import redis
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = (ROOT / "Dockerfile").read_text()
RENDER = (ROOT / "render.yaml").read_text()
HEROKU = (ROOT / "heroku.yml").read_text()
APP = json.loads((ROOT / "app.json").read_text())
# Plain scalars YAML 1.1 parsers read as something other than a string (off, yes, 6004), or that start an indicator.
AMBIGUOUS = re.compile(r"(?i)(y|n|yes|no|on|off|true|false|null|~|[-+.\d].*)|[-?:,\[\]{}#&*!|>'\"%@`].*|.*(: | #).*")


def scalar(text: str):
    if text in ("[]", "true", "false"):
        return {"[]": [], "true": True, "false": False}[text]
    if text.startswith('"'):
        assert re.fullmatch(r'"[^"\\]*"', text), text
        return text[1:-1]
    assert not AMBIGUOUS.fullmatch(text), f"quote {text!r}"
    return text


def load_yaml(text: str):
    """Block mappings and sequences of the scalars above, and nothing else, so every YAML parser reads them alike."""
    lines = [(len(s) - len(s.lstrip(" ")), s.strip()) for s in text.splitlines() if s.strip() and s.strip()[0] != "#"]
    node, end = _block(lines, 0)
    assert end == len(lines), lines[end]
    return node


def _block(lines: list[tuple[int, str]], i: int):
    indent, seq = lines[i][0], lines[i][1].startswith("- ")
    out = [] if seq else {}
    while i < len(lines) and lines[i][0] == indent and lines[i][1].startswith("- ") == seq:
        text = lines[i][1].removeprefix("- ") if seq else lines[i][1]
        if seq and not re.match(r"\w+:( |$)", text):
            out.append(scalar(text))
            i += 1
        elif seq:
            lines[i] = (indent + 2, text)
            item, i = _block(lines, i)
            out.append(item)
        else:
            key, rest = re.fullmatch(r"(\w+):(?: (.+))?", text).groups()
            assert key not in out, key
            if rest:
                out[key], i = scalar(rest), i + 1
            else:
                assert lines[i + 1][0] > indent, lines[i]
                out[key], i = _block(lines, i + 1)
    return out, i


def render_problems(blueprint: dict) -> list[str]:
    """What the Blueprint spec or HS would trip on, each check named for its reason."""
    web, kv = blueprint["services"]
    [pg] = blueprint["databases"]
    env = {var["key"]: {k: v for k, v in var.items() if k != "key"} for var in web["envVars"]}
    web_keys = {"type", "name", "runtime", "plan", "autoDeployTrigger", "healthCheckPath", "dockerCommand", "envVars"}
    own_url = {"type": "web", "name": web["name"], "envVarKey": "RENDER_EXTERNAL_URL"}
    checks = {
        "a web service, a key value store and a database": (web.get("type"), kv.get("type"), "type" in pg)
        == ("web", "keyvalue", False),
        "only the keys the sheet justifies": set(web) == web_keys
        and set(kv) == {"type", "name", "plan", "maxmemoryPolicy", "ipAllowList"}
        and set(pg) == {"name", "plan", "ipAllowList"},
        "free plans": web["plan"] == kv.get("plan") == pg.get("plan") == "free",
        "Docker: Render's Python runtime sets FORWARDED_ALLOW_IPS=*": web["runtime"] == "docker",
        "no redeploy on every push": web["autoDeployTrigger"] == "off",
        "health check": web.get("healthCheckPath") == "/status",
        "one word, however Render splits a command": web["dockerCommand"] == "/app/migrate-and-serve",
        "Render routes to PORT": env["PORT"] == {"value": "6004"},
        "one listening port": env["METRICS_PORT"] == {"value": "0"},
        "database": env["DATABASE_URL"] == {"fromDatabase": {"name": pg["name"], "property": "connectionString"}},
        "redis": env["REDIS_URL"]
        == {"fromService": {"type": "keyvalue", "name": kv["name"], "property": "connectionString"}},
        "own URL": env["PUBLIC_BASE_URL"] == {"fromService": own_url},
        "generated keys": env["HS_OPERATOR_KEY"] == env["HS_SECRET_KEY"] == {"generateValue": True},
        "the one field the form asks for": env["HS_SETUP_PLIVO_AUTH_ID"] == {"sync": False},
        "Render's default policy evicts": kv.get("maxmemoryPolicy") == "noeviction",
        "ipAllowList required, empty is internal only": kv.get("ipAllowList") == pg.get("ipAllowList") == [],
    }
    return [name for name, ok in checks.items() if not ok]


def test_the_render_blueprint_asks_render_for_what_hs_needs():
    assert render_problems(load_yaml(RENDER)) == []


@pytest.mark.parametrize(
    ("good", "bad"),
    [
        ("noeviction", "noevict"),
        ("healthCheckPath", "healthcheckPath"),
        ("    ipAllowList: []\ndatabases", "databases"),
        ("plan: free", "plan: tiny"),
        ("- type: web", "- type: typo"),
        ("type: keyvalue\n    name", "type: redis\n    name"),
    ],
)
def test_a_broken_blueprint_is_caught(good, bad):
    assert good in RENDER and render_problems(load_yaml(RENDER.replace(good, bad)))


@pytest.mark.parametrize(("good", "bad"), [('"off"', "off"), ('"6004"', "6004"), ("/status", "/status # health")])
def test_a_value_yaml_parsers_read_differently_is_refused(good, bad):
    with pytest.raises(AssertionError):
        load_yaml(RENDER.replace(good, bad, 1))


@pytest.fixture
def stubs(tmp_path):
    for tool in ("alembic", "uvicorn"):
        log = f'echo "$$ {tool} $*" >> {tmp_path}/calls\nenv > {tmp_path}/{tool}.env'
        (tmp_path / tool).write_text(f"#!/bin/sh\n{log}\nexit ${{EXIT:-0}}\n")
        (tmp_path / tool).chmod(0o755)
    return tmp_path


def run_logged(stubs, argv: list, **env) -> tuple[subprocess.Popen, list[list[str]]]:
    run = subprocess.Popen(argv, env={"PATH": f"{stubs}:/usr/bin:/bin", **env})
    run.wait()
    calls = (stubs / "calls").read_text().splitlines()
    (stubs / "calls").unlink()
    return run, [line.split(" ", 1) for line in calls]


def seen(stubs, tool: str) -> dict:
    return dict(line.split("=", 1) for line in (stubs / f"{tool}.env").read_text().splitlines() if "=" in line)


CMD = " ".join(json.loads(re.search(r"^CMD (.+)$", DOCKERFILE, re.M).group(1)))
START, HEROKU_START = [ROOT / "deploy/migrate-and-serve"], [ROOT / "deploy/heroku"]


def test_the_image_scripts_are_stored_executable_and_copied_into_app_as_they_are():
    scripts = ["deploy/migrate-and-serve", "deploy/heroku"]
    stored = subprocess.run(["git", "ls-files", "-s", *scripts], cwd=ROOT, capture_output=True, text=True, check=True)
    assert [line.split()[0] for line in stored.stdout.splitlines()] == ["100755"] * len(scripts), stored.stdout
    copy = re.search(r"^WORKDIR /app\n(?:.*\n)*COPY (.+) \./$", DOCKERFILE, re.M).group(1).split()
    assert set(scripts) <= set(copy) and "chmod" not in DOCKERFILE


def test_the_start_script_migrates_then_becomes_the_images_service_on_the_platforms_port(stubs):
    run, calls = run_logged(stubs, START)
    assert run.returncode == 0 and calls == [[calls[0][0], "alembic upgrade head"], [str(run.pid), CMD]]
    run, calls = run_logged(stubs, START, PORT="41234")
    assert calls[-1] == [str(run.pid), CMD.replace("--port 6004", "--port 41234")]


def test_the_start_script_never_serves_after_a_failed_migration(stubs):
    run, calls = run_logged(stubs, START, EXIT="1")
    assert run.returncode == 1 and [tool for _, tool in calls] == ["alembic upgrade head"]


REPO = "https://github.com/plivo-labs/human-specialist"
# The add-ons' URLs in the shape Heroku's Postgres and Key-Value Store pages show.
HEROKU_DATABASE = "postgres://u4a2:p%40ss@db.example.com:5432/d9fm"
HEROKU_REDIS = "rediss://:p4e08c@kv.example.com:17749"


# The keys Heroku's app.json schema documents.
APP_KEYS = {"name", "description", "keywords", "website", "repository", "logo", "success_url", "scripts", "env"}
APP_KEYS |= {"formation", "image", "addons", "buildpacks", "environments", "stack"}


def test_the_heroku_app_asks_only_for_the_auth_id_and_says_where_to_set_the_url():
    env, manifest = APP["env"], load_yaml(HEROKU)
    fields = {name: value for name, value in env.items() if isinstance(value, dict)}
    assert set(APP) <= APP_KEYS and len(APP["name"]) <= 30 and all(isinstance(v, str | dict) for v in env.values())
    for field in fields.values():
        assert set(field) <= {"description", "value", "required", "generator"}, field
        assert field.get("generator", "secret") == "secret" and isinstance(field.get("required", True), bool)
    asked = [name for name, f in fields.items() if f.get("required", True) and not {"value", "generator"} & set(f)]
    assert asked == ["HS_SETUP_PLIVO_AUTH_ID"]
    assert {name for name, f in fields.items() if "generator" in f} == {"HS_OPERATOR_KEY", "HS_SECRET_KEY"}
    assert (
        fields["PUBLIC_BASE_URL"]["required"] is False
        and "After the deploy" in fields["PUBLIC_BASE_URL"]["description"]
    )
    assert "PUBLIC_BASE_URL" in APP["description"] and APP["success_url"] == f"{REPO}/blob/main/docs/deploy.md#heroku"
    assert re.search(r"^## Heroku$", (ROOT / "docs/deploy.md").read_text(), re.M)
    pool = int(env["DB_POOL_SIZE"]) + int(env["DB_MAX_OVERFLOW"])
    # HS refuses a pool under 16; Essential-0's 20 connections hold it, HS's 2 for numbers and the release phase's 1.
    assert pool >= 16 and pool + 2 + 1 <= 20 and env["METRICS_PORT"] == "0"
    assert APP["formation"] == {"web": {"quantity": 1, "size": "basic"}} and APP["stack"] == "container"
    assert [addon["plan"] for addon in APP["addons"]] == ["heroku-postgresql:essential-0", "heroku-redis:mini"]
    assert manifest["build"] == {"docker": {"web": "Dockerfile"}} and manifest["release"]["image"] == "web"


def test_heroku_yml_runs_the_images_heroku_script_in_each_phase():
    manifest = load_yaml(HEROKU)
    assert manifest["release"]["command"] == ["/app/heroku release"] and manifest["run"] == {"web": "/app/heroku web"}


def test_the_heroku_release_migrates_with_a_database_url_sqlalchemy_accepts(stubs):
    run, calls = run_logged(stubs, [*HEROKU_START, "release"], DATABASE_URL=HEROKU_DATABASE, REDIS_URL=HEROKU_REDIS)
    database = seen(stubs, "alembic")["DATABASE_URL"]
    assert run.returncode == 0 and calls == [[str(run.pid), "alembic upgrade head"]]
    assert database == HEROKU_DATABASE.replace("postgres://", "postgresql://")
    with pytest.raises(NoSuchModuleError):
        make_url(HEROKU_DATABASE).get_dialect()
    assert make_url(database).get_dialect().driver == "psycopg2"
    run_logged(stubs, [*HEROKU_START, "release"], DATABASE_URL=database)
    assert seen(stubs, "alembic")["DATABASE_URL"] == database


@pytest.mark.parametrize(
    ("given", "served"),
    [
        (HEROKU_REDIS, f"{HEROKU_REDIS}?ssl_cert_reqs=none"),
        (f"{HEROKU_REDIS}?health_check_interval=10", f"{HEROKU_REDIS}?health_check_interval=10&ssl_cert_reqs=none"),
        (f"{HEROKU_REDIS}?ssl_cert_reqs=required", f"{HEROKU_REDIS}?ssl_cert_reqs=required"),
        (f"{HEROKU_REDIS}?db=0&ssl_cert_reqs=none", f"{HEROKU_REDIS}?db=0&ssl_cert_reqs=none"),
        ("redis://:pw@10.0.0.5:6379", "redis://:pw@10.0.0.5:6379"),
    ],
)
def test_the_heroku_web_process_serves_on_port_with_a_redis_url_redis_py_accepts(stubs, given, served):
    run, calls = run_logged(stubs, [*HEROKU_START, "web"], DATABASE_URL=HEROKU_DATABASE, REDIS_URL=given, PORT="41234")
    assert calls == [[str(run.pid), CMD.replace("--port 6004", "--port 41234")]]
    assert seen(stubs, "uvicorn")["REDIS_URL"] == served
    assert seen(stubs, "uvicorn")["DATABASE_URL"].startswith("postgresql://")
    pool = redis.Redis.from_url(served).connection_pool
    connection = pool.connection_class(**pool.connection_kwargs)
    assert isinstance(connection, redis.SSLConnection) == served.startswith("rediss://")
    if served.endswith("ssl_cert_reqs=none"):
        assert connection.cert_reqs == ssl.CERT_NONE and connection.check_hostname is False


BUTTONS = {"https://render.com/deploy?repo=": "render.yaml", "https://www.heroku.com/deploy?template=": "app.json"}


def test_every_deploy_button_names_this_repository_whose_root_holds_the_file_it_reads():
    text = "".join(page.read_text() for page in [ROOT / "README.md", *(ROOT / "docs").glob("*.md")])
    for prefix, name in BUTTONS.items():
        urls = re.findall(rf"\]\({re.escape(prefix)}([^)]*)\)", text)
        assert urls and all(re.fullmatch(rf"{REPO}(/tree/[\w.-]+)?", url) for url in urls), (prefix, urls)
        assert (ROOT / name).is_file(), name


def test_the_readme_deploy_row_has_a_badge_for_each_platform_the_deploy_page_covers():
    deploy = (ROOT / "docs/deploy.md").read_text()
    platforms = re.findall(
        r"^## (.+)$", deploy.split("\n## Run it on your platform\n")[1].split("\n## Host the UI")[0], re.M
    )
    row = (ROOT / "README.md").read_text().split("\n## Deploy\n")[1].split("\n## ")[0]
    assert len(platforms) >= 5 and all(f"badge/deploy-{p.replace(' ', '%20')}-" in row for p in platforms), platforms


def test_every_variable_a_deploy_file_or_the_railway_template_sets_is_documented():
    documented = set(re.findall(r"^\| `(\w+)` \|", (ROOT / "docs/configuration.md").read_text(), re.M))
    render = {var["key"] for var in load_yaml(RENDER)["services"][0]["envVars"]}
    heroku = set(re.findall(r"\$\{?([A-Z_]+)", (ROOT / "deploy/heroku").read_text()))
    railway = (ROOT / "docs/deploy.md").read_text().split("\n## Railway\n")[1].split("\n## ")[0]
    template = set(re.findall(r"`([A-Z_]+)`", "".join(re.findall(r"^\| `.*?\|", railway, re.M))))
    assert heroku == {"DATABASE_URL", "REDIS_URL", "PORT"} and "HS_SETUP_PLIVO_AUTH_ID" in template
    assert (render | set(APP["env"]) | heroku | template) - documented == set()
