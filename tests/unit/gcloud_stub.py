"""A stand-in for `gcloud` that deploy/gcp/precreate.sh can run against: it keeps which resources exist in a JSON
file, records every call, and refuses any command it does not know."""

import json
import os
import sys
from pathlib import Path

home = Path(os.environ["GCLOUD_STUB"])
state = json.loads((home / "state.json").read_text())
args = sys.argv[1:]
with (home / "calls").open("a") as calls:
    calls.write(json.dumps(args) + "\n")
words = [a for a in args if not a.startswith("-")]
flags = dict(a[2:].split("=", 1) for a in args if a.startswith("--") and "=" in a)
for prefix, message in state.get("fail", {}).items():
    if " ".join(args).startswith(prefix):
        sys.exit(message)

match words:
    case ["services", "enable", *_] | ["projects", "add-iam-policy-binding", _]:
        pass
    case ["projects", "describe", _]:
        sys.stdout.write("123456789012\n")
    case ["redis", "instances", "get-auth-string", _]:
        sys.stdout.write(state.get("auth", "6d0b4b6e-0f5c-4ab0-9b1e-1f2d3c4b5a69") + "\n")
    case ["secrets", "versions", "access", "1"]:
        sys.stdout.write(state["secrets"][flags["secret"]])
    case ["secrets", "versions", "describe", "1"] if flags["secret"] not in state["secrets"]:
        sys.exit("ERROR: NOT_FOUND")
    case ["secrets", "versions", "describe", "1"]:
        pass
    case ["secrets", "versions", "add", name] if f"secrets {name}" in state["have"]:
        state["secrets"].setdefault(name, sys.stdin.read())
    case [*group, "describe" | "create" as verb, name]:
        if group == ["iam", "service-accounts"] and verb == "create":
            name = f"{name}@demo.iam.gserviceaccount.com"
        if "instance" in flags:
            name = f"{flags['instance']}/{name}"
        thing = " ".join([*group, name])
        if verb == "describe":
            if thing not in state["have"]:
                sys.exit("ERROR: NOT_FOUND")
            if "format" in flags:
                sys.stdout.write(f"{state.get('redis', 'READY')}\t{state.get('host', '10.0.0.3')}\n")
        elif thing in state["have"]:
            sys.exit("ERROR: ALREADY_EXISTS")
        else:
            state["have"].append(thing)
            if group == ["sql", "users"]:
                state["passwords"][name] = Path(flags["flags-file"]).read_text().removeprefix("--password: ").strip()
    case ["run", "deploy", _]:
        state["deployed"] = args
    case _:
        sys.exit(f"gcloud stub: unexpected {args}")
(home / "state.json").write_text(json.dumps(state))
