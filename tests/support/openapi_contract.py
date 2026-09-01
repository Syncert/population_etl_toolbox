"""A reviewable digest of the API's public OpenAPI contract.

A raw ``app.openapi()`` dump is too large and too noisy to review, so a drifting
field hides in it. This module distills the document to the parts a consumer can
actually depend on -- which operations exist, what each one accepts, and the
exact shape of every response schema -- and renders them in a stable order so a
snapshot diff reads as a contract change rather than a JSON reshuffle.

The digest is deliberately lossy in one direction only: everything it keeps is
something a client can break on. Descriptions, examples, and titles are dropped;
names, types, nullability, requiredness, and validation bounds are not.
"""

from __future__ import annotations

from typing import Any

#: Keys on a parameter's schema that bound what a client may send. A change to
#: any of them is a contract change even when the field name is untouched.
_CONSTRAINT_KEYS: tuple[str, ...] = (
    "maximum",
    "minimum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "maxLength",
    "minLength",
    "pattern",
    "enum",
    "default",
)


def _ref_name(schema: dict[str, Any]) -> str | None:
    reference = schema.get("$ref")
    if not isinstance(reference, str):
        return None
    return reference.rsplit("/", 1)[-1]


def type_expression(schema: dict[str, Any] | None) -> str:
    """Render a schema as one readable type expression.

    ``anyOf`` carrying ``null`` is how OpenAPI 3.1 spells an optional field, so
    it collapses to a ``| null`` suffix rather than an anonymous union -- that
    keeps a genuine union (a field that really does accept two shapes) visible
    instead of blending it into the noise of nullability.
    """
    if not schema:
        return "any"

    referenced = _ref_name(schema)
    if referenced:
        return referenced

    if "anyOf" in schema or "oneOf" in schema:
        members = schema.get("anyOf") or schema.get("oneOf") or []
        rendered = [type_expression(member) for member in members]
        nullable = "null" in rendered
        remaining = [name for name in rendered if name != "null"]
        joined = " | ".join(remaining) if remaining else "any"
        return f"{joined} | null" if nullable else joined

    declared = schema.get("type")
    if declared == "array":
        return f"array<{type_expression(schema.get('items'))}>"
    if isinstance(declared, str):
        declared_format = schema.get("format")
        return f"{declared}({declared_format})" if declared_format else declared
    return "any"


def _collect_constraints(schema: dict[str, Any]) -> dict[str, Any]:
    """Gather validation bounds from a schema and its non-null union members.

    An optional string parameter renders in OpenAPI 3.1 as
    ``anyOf: [{type: string, maxLength: 200}, {type: "null"}]``, so its bound sits
    one level down. Reading only the top level silently dropped ``maxLength`` from
    every optional filter -- which is most of them -- and left the snapshot blind
    to exactly the drift it exists to catch.
    """
    collected: dict[str, Any] = {
        key: schema[key] for key in _CONSTRAINT_KEYS if key in schema
    }
    for member in schema.get("anyOf") or schema.get("oneOf") or []:
        if not isinstance(member, dict) or member.get("type") == "null":
            continue
        for key in _CONSTRAINT_KEYS:
            if key in member and key not in collected:
                collected[key] = member[key]
    return collected


def _parameter_digest(parameter: dict[str, Any]) -> dict[str, Any]:
    schema = parameter.get("schema") or {}
    digest: dict[str, Any] = {
        "name": parameter.get("name"),
        "in": parameter.get("in"),
        "required": bool(parameter.get("required", False)),
        "type": type_expression(schema),
    }
    constraints = _collect_constraints(schema)
    # An optional query parameter's default is ``None``; recording it would add a
    # line of noise to every parameter without describing a real bound.
    if constraints.get("default") is None:
        constraints.pop("default", None)
    if constraints:
        digest["constraints"] = constraints
    return digest


def _response_digest(responses: dict[str, Any]) -> dict[str, str]:
    digest: dict[str, str] = {}
    for status, response in sorted(responses.items()):
        content = (response or {}).get("content") or {}
        if not content:
            digest[status] = "no-content"
            continue
        for media_type, media in sorted(content.items()):
            digest[status] = f"{media_type}:{type_expression(media.get('schema'))}"
    return digest


def _schema_digest(schema: dict[str, Any]) -> dict[str, Any]:
    required = sorted(schema.get("required") or [])
    properties = schema.get("properties") or {}
    return {
        "required": required,
        "properties": {
            name: type_expression(properties[name]) for name in sorted(properties)
        },
    }


def contract_digest(document: dict[str, Any]) -> dict[str, Any]:
    """Reduce an OpenAPI document to its reviewable public contract."""
    operations: dict[str, Any] = {}
    for path, path_item in (document.get("paths") or {}).items():
        for method, operation in (path_item or {}).items():
            if method.startswith("x-"):
                continue
            key = f"{method.upper()} {path}"
            operations[key] = {
                "tags": sorted(operation.get("tags") or []),
                "parameters": [
                    _parameter_digest(parameter)
                    for parameter in sorted(
                        operation.get("parameters") or [],
                        key=lambda item: (item.get("in", ""), item.get("name", "")),
                    )
                ],
                "responses": _response_digest(operation.get("responses") or {}),
            }

    schemas = (document.get("components") or {}).get("schemas") or {}
    return {
        "openapi": document.get("openapi"),
        "operations": {key: operations[key] for key in sorted(operations)},
        "schemas": {name: _schema_digest(schemas[name]) for name in sorted(schemas)},
    }


def describe_difference(expected: dict[str, Any], actual: dict[str, Any]) -> str:
    """Explain a digest mismatch in terms a reviewer can act on."""
    lines: list[str] = []
    for section in ("operations", "schemas"):
        expected_section = expected.get(section) or {}
        actual_section = actual.get(section) or {}
        for name in sorted(set(actual_section) - set(expected_section)):
            lines.append(f"added {section[:-1]}: {name}")
        for name in sorted(set(expected_section) - set(actual_section)):
            lines.append(f"removed {section[:-1]}: {name}")
        for name in sorted(set(expected_section) & set(actual_section)):
            if expected_section[name] != actual_section[name]:
                lines.append(
                    f"changed {section[:-1]}: {name}\n"
                    f"    reviewed: {expected_section[name]}\n"
                    f"    current:  {actual_section[name]}"
                )
    if expected.get("openapi") != actual.get("openapi"):
        lines.append(
            f"openapi version {expected.get('openapi')} -> {actual.get('openapi')}"
        )
    return "\n".join(lines) or "no structural difference"
