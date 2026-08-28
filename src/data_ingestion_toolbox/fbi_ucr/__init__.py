"""FBI Uniform Crime Reporting (UCR) ingestion package.

This package ingests public FBI UCR data published through the official
Crime Data Explorer (CDE) API. It follows the capture-first, replayable
data-layer design: every provider response is committed to the shared
``raw_capture`` objects before any parsing, and silver is rebuilt offline
from those stored captures.

The pipeline deliberately keeps three source distinctions explicit:

* provider-published national and state observations are consumed only from
  their own endpoints and never reconstructed by summing agencies;
* agency observations stay at Originating Agency Identifier (ORI) grain, and
  county/place associations are evidence-backed filters rather than totals; and
* a month without a report is recorded as not reported, never as zero.

The shared raw-capture and control-plane primitives come from
:mod:`data_ingestion_toolbox.capture` and are never reimplemented here.
"""
