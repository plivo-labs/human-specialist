import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "deploy/aws/template.yaml"
TEXT = TEMPLATE.read_text()


def undocumented(text: str) -> set[str]:
    names = set(re.findall(r"-\s+(?:\{\s*)?Name: ([A-Z][A-Z0-9_]*)\b", text))
    assert "DATABASE_URL" in names, "no container variable found in the template"
    return names - set(re.findall(r"`([A-Z][A-Z0-9_]+)`", (ROOT / "docs/configuration.md").read_text()))


def family_matches_engine(text: str) -> bool:
    family = re.search(r"CacheParameterGroupFamily: (\S+)", text)[1]
    engine, major = re.search(r"Engine: (valkey)\n\s+EngineVersion: \"(\d+)\.", text).groups()
    return family == f"{engine}{major}"


def test_the_template_runs_this_repository_image_at_the_project_version_and_a_bump_moves_it():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert f"Default: ghcr.io/plivo-labs/human-specialist:{project['project']['version']}\n" in TEXT
    entry = "deploy/aws/template.yaml:ghcr.io/plivo-labs/human-specialist:"
    assert entry in project["tool"]["commitizen"]["version_files"]


def test_every_variable_the_task_sets_is_one_the_configuration_page_lists():
    assert undocumented(TEXT) == set()
    assert undocumented(TEXT + "            - { Name: HS_NOT_A_SETTING, Value: x }\n") == {"HS_NOT_A_SETTING"}


def test_the_cache_parameter_family_is_the_engine_major_version():
    # cfn-lint accepts any family name, and a wrong one fails only when the stack creates the cache.
    assert family_matches_engine(TEXT)
    assert not family_matches_engine(TEXT.replace("valkey8", "valkey9x"))


def cache_never_evicts(text: str) -> bool:
    name = re.search(r"CacheParameterGroupName: !Ref (\w+)", text)
    group = name and re.search(rf"^  {name[1]}:\n((?:    .*\n)+)", text, re.M)
    return bool(group) and "maxmemory-policy: noeviction\n" in group[1]


def test_the_cache_uses_a_parameter_group_that_never_evicts():
    # The default group evicts (volatile-lru), and HS never rebuilds a key Redis evicted.
    assert cache_never_evicts(TEXT)
    assert not cache_never_evicts(TEXT.replace("      CacheParameterGroupName: !Ref CacheParameters\n", ""))
    assert not cache_never_evicts(TEXT.replace("maxmemory-policy: noeviction", "maxmemory-policy: volatile-lru"))


def private_front_door(text: str) -> bool:
    origins = re.findall(r"^\s+- Id: ", text, re.M)
    return re.findall(r"Scheme: (\S+)", text) == ["internal"] and text.count("VpcOriginConfig:") == len(origins) > 0


def test_cloudfront_reaches_the_load_balancer_only_through_a_vpc_origin():
    # An internet-facing load balancer, the default when Scheme is absent, would carry tokens over the internet in clear.
    assert private_front_door(TEXT)
    assert not private_front_door(TEXT.replace("Scheme: internal", "Scheme: internet-facing"))
    assert not private_front_door(TEXT.replace("      Scheme: internal\n", ""))
    assert not private_front_door(TEXT.replace("VpcOriginConfig:", "CustomOriginConfig:"))


def test_the_template_deploys_without_a_bucket():
    # `aws cloudformation deploy --template-file` sends the template inline, which CloudFormation caps at 51,200 bytes.
    assert len(TEMPLATE.read_bytes()) <= 51_200


def test_the_deploy_page_names_the_template_where_it_is():
    doc = (ROOT / "docs/deploy.md").read_text()
    paths = set(re.findall(r"deploy/aws/[\w./-]*\w", doc))
    assert "deploy/aws/template.yaml" in paths
    assert {p for p in paths if not (ROOT / p).is_file()} == set()
    raw = re.findall(r"https://raw\.githubusercontent\.com/plivo-labs/human-specialist/v<version>/(\S+)", doc)
    assert raw == ["deploy/aws/template.yaml"]
