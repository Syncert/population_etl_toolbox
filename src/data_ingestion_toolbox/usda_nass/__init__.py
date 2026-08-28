"""USDA National Agricultural Statistics Service (NASS) Quick Stats adapter.

The package is capture-first: every registered slice is preflighted against the
provider count facility, captured losslessly, and only then replayed into the
silver and gold contracts. Nothing here reads ``USDA_NASS_API_KEY`` at import
time; the secret is resolved and validated when a request actually executes.
"""

from __future__ import annotations

SOURCE_CODE = "USDA_NASS"

__all__ = ["SOURCE_CODE"]
