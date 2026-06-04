from dataclasses import dataclass


@dataclass(frozen=True)
class Period:
    value: str
    frequency: str = "annual"
