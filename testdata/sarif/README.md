# SARIF test fixtures

This directory contains synthetic SARIF 2.1.0 inputs for parser and ingestion
tests. Every fixture was written specifically for Open ASPM and is licensed
under `Apache-2.0`, like the rest of the repository. No fixture is derived from
customer data or scanner output.

The examples use fictional names, relative source paths, reserved domains, and
non-secret values. They were reviewed for sensitive data on 2026-09-22.

## Manifest

| Name | Purpose | Expected result | Origin | Modifications |
| --- | --- | --- | --- | --- |
| `valid/minimal.sarif.json` | Smallest useful SARIF log; optional fields are absent | Import successfully | Synthetic | None |
| `valid/representative.sarif.json` | Multiple runs, rules, locations, fingerprints, artifacts, and absent optional result fields | Import successfully | Synthetic | None |
| `invalid/malformed-json.sarif.json` | Truncated JSON object | Reject as malformed JSON | Synthetic | Intentionally truncated |
| `invalid/unsupported-version.sarif.json` | Well-formed log with an unsupported SARIF version | Reject as an unsupported version | Synthetic | Uses the fictional version `9.9.9` |
| `invalid/deeply-nested.sarif.json` | Bounded nested property bag for testing a nesting guard | Reject when the configured nesting limit is 16 | Synthetic | Contains 24 finite object levels |
| `invalid/oversized.sarif.json` | Bounded input for testing a byte-size guard | Reject when the configured test limit is 1 KiB | Synthetic | Contains harmless padding and remains below the repository's 256 KiB fixture limit |

The 1 KiB size threshold and nesting limit of 16 are fixture-test boundaries,
not proposed production limits. Production limits belong to the ingestion
contract and configuration. Resource-exhaustion tests that require larger data
must generate it at test time according to
[`docs/testing/test-data-policy.md`](../../docs/testing/test-data-policy.md).

The valid examples identify the normative SARIF 2.1.0 schema through their
`$schema` property, but the schema itself is not copied into this repository.
