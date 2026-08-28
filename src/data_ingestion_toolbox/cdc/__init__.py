"""CDC illness and disease ingestion package.

This package ingests CDC public data products published through the
CDC Open Data (Socrata) portal, beginning with the U.S. Chronic Disease
Indicators (CDI) product. It follows the capture-first, replayable
data-layer design: every successful provider response is committed to the
shared ``raw_capture`` objects before any parsing, and silver is rebuilt
offline from stored captures.

The shared raw-capture and control-plane primitives come from
:mod:`data_ingestion_toolbox.capture` and are never reimplemented here.
"""
