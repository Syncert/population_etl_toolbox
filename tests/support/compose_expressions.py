"""Reading Compose's `${VAR}` interpolation the way Compose reads it.

Two deployment guards need the same two answers out of a compose file: which
variables it interpolates and with what kind of default, and what a value
resolves to for an operator who runs `docker compose up` with nothing set.
A regex answers both wrongly, because Compose allows a reference inside
another reference's default -- `${PUBLIC_DATA_DB_NAME:-${ANALYTICS_DB_NAME}}`
is one reference whose default is another -- and a regex reads that outer
default as empty, which turns a working fallback into a missing value.

So the braces are matched by hand, here, once.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Interpolation:
    """One `${...}` reference and whether a value has to be supplied for it."""

    name: str
    #: `":-"`, `":?"`, or `""` when the reference names no default at all.
    separator: str
    default: str

    @property
    def must_be_supplied(self) -> bool:
        """True when nothing usable stands behind the reference.

        A `:?` reference carries the message Compose prints when the value is
        missing rather than a fallback, so it needs a value exactly as much as
        a bare `${VAR}` does.
        """
        return self.separator != ":-" or self.default == ""


def closing_brace(text: str, start: int) -> int:
    """The index of the `}` closing the `${` that begins at ``start``."""
    depth = 0
    cursor = start
    while cursor < len(text):
        if text.startswith("${", cursor):
            depth += 1
            cursor += 2
            continue
        if text[cursor] == "}":
            depth -= 1
            if depth == 0:
                return cursor
        cursor += 1
    return len(text) - 1


def split_default(body: str) -> tuple[str, str, str]:
    """Split a reference body on its own `:-` or `:?`, not on a nested one."""
    depth = 0
    for position, character in enumerate(body):
        if body.startswith("${", position):
            depth += 1
        elif character == "}" and depth:
            depth -= 1
        elif (
            character == ":"
            and depth == 0
            and body[position + 1 : position + 2] in {"-", "?"}
        ):
            return body[:position], body[position : position + 2], body[position + 2 :]
    return body, "", ""


def interpolations(text: str) -> list[Interpolation]:
    """Every `${...}` reference in ``text``, nested ones included."""
    found: list[Interpolation] = []
    index = 0
    while True:
        start = text.find("${", index)
        if start == -1:
            return found
        end = closing_brace(text, start)
        body = text[start + 2 : end]
        name, separator, default = split_default(body)
        if name.isidentifier() and name.isupper():
            found.append(Interpolation(name, separator, default))
        # Re-scan the body so a nested reference is read on its own terms.
        found.extend(interpolations(body))
        index = end + 1


def resolve(expression: str) -> str:
    """What ``expression`` becomes for `docker compose up` with nothing set.

    This is the shipped stack as an operator first meets it: no `.env`, no
    exported variables, every reference falling through to its default. A
    reference with no default resolves to the empty string, which is what
    Compose substitutes (with a warning) rather than refusing to start.
    """
    result = []
    index = 0
    while index < len(expression):
        if expression.startswith("${", index):
            end = closing_brace(expression, index)
            _, separator, default = split_default(expression[index + 2 : end])
            result.append(resolve(default) if separator == ":-" else "")
            index = end + 1
            continue
        result.append(expression[index])
        index += 1
    return "".join(result)
