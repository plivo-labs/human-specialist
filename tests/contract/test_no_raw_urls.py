"""Responses never carry a raw provider URL: the only `*_url` fields are HS's own signed or action URLs."""

ALLOWED = {"enqueue_url", "menu_url", "audio_url"}


def properties(spec: dict, schema: dict, seen: set):
    """Every property name reachable from `schema`."""
    if ref := schema.get("$ref"):
        name = ref.rsplit("/", 1)[-1]
        if name not in seen:
            seen.add(name)
            yield from properties(spec, spec["components"]["schemas"][name], seen)
        return
    for prop, sub in schema.get("properties", {}).items():
        yield prop
        yield from properties(spec, sub, seen)
    for key in ("items", "additionalProperties"):
        if isinstance(schema.get(key), dict):
            yield from properties(spec, schema[key], seen)
    for key in ("allOf", "anyOf", "oneOf"):
        for sub in schema.get(key, []):
            yield from properties(spec, sub, seen)


def test_no_response_field_is_a_raw_url(app):
    spec, bad = app.openapi(), []
    for path, ops in spec.get("paths", {}).items():
        for method, op in ops.items():
            for status, response in op.get("responses", {}).items():
                for media in response.get("content", {}).values():
                    names = properties(spec, media.get("schema", {}), set())
                    bad += [
                        f"{method.upper()} {path} {status}: {n}"
                        for n in names
                        if n.endswith("_url") and n not in ALLOWED
                    ]
    assert not bad, "\n".join(sorted(set(bad)))
