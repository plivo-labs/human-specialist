"""Every documented non-2xx response of a public (/api/v1) route is the error envelope `{"error": {code, message, ...}, "request_id"}`."""


def resolve(spec: dict, schema: dict) -> dict:
    while "$ref" in schema:
        schema = spec["components"]["schemas"][schema["$ref"].rsplit("/", 1)[-1]]
    return schema


def is_envelope(spec: dict, schema: dict) -> bool:
    props = resolve(spec, schema).get("properties", {})
    if not {"error", "request_id"} <= props.keys():
        return False
    return {"code", "message"} <= resolve(spec, props["error"]).get("properties", {}).keys()


def test_non_2xx_responses_use_the_error_envelope(app):
    spec, bad = app.openapi(), []
    for path, ops in spec.get("paths", {}).items():
        if not path.startswith("/api/v1"):
            continue
        for method, op in ops.items():
            for status, response in op.get("responses", {}).items():
                schema = response.get("content", {}).get("application/json", {}).get("schema")
                if not status.startswith("2") and not (schema and is_envelope(spec, schema)):
                    bad.append(f"{method.upper()} {path} {status}")
    assert not bad, "responses not using the error envelope:\n" + "\n".join(bad)
