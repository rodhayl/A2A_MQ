# Phase 1 validation

Executed `python tools/check.py` and `python -m pytest -q`: **121 passed, 2 skipped**.
The skips are explicitly marked optional upstream protobuf/SDK round trips; the
SDK is not installed in this offline environment. CI installs the pinned extra.

Tests include every state pair, terminal reachability, malformed envelope fuzzing
with a fixed seed, ambiguous/unsafe routes, deep/duplicate/non-finite JSON, byte
limits, A2A identifiers, legacy-shape rejection, expiry boundaries, trace IDs,
immutable defensive copies, snapshot/status/artifact codecs and factory routing.
This is structural payload validation, not full public A2A binding conformance.

The JSON Schema format extra is now explicit so clean installs enforce the same
format checks as the local preinstalled environment. Ruff/mypy and a fresh network
resolver still could not be run here. No network code or broker claim is involved.
