"""Census Population Estimates Program (PEP) adapter package.

This package implements ingestion of Census PEP products including:
- Annual population estimates (totals and components of change)
- Population estimates for national, state, county, and place geographies
- Vintage-aware revision tracking

The adapter follows the shared capture/control foundation and preserves
source fidelity across vintages.
"""

from __future__ import annotations

__version__ = "0.1.0"
