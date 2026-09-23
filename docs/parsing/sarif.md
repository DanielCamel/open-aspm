# SARIF parsing

Open ASPM's first format adapter is `internal/parsing/sarif`. It accepts SARIF
2.1.0 and emits a deterministic, source-oriented representation for later
scan and observation processing. The adapter follows the
[OASIS SARIF 2.1.0 standard](https://docs.oasis-open.org/sarif/sarif/v2.1.0/os/sarif-v2.1.0-os.html).

The parser is not an Observation store, normalizer, correlation engine, or
finding lifecycle owner. Those responsibilities remain in their respective
bounded contexts.

## Version and supported projection

Parser behavior is identified by:

```text
format:         sarif
format_version: 2.1.0
parser_version: 1
```

Version 1 maps:

- runs and their source JSON pointers;
- tool driver identity and version fields;
- automation identifiers;
- invocation success/failure evidence and source timestamps;
- rule identifiers, descriptions, help, and default level;
- result identifiers, rule references, level, kind, baseline state, and
  messages;
- physical artifact locations and line/column regions; and
- complete and partial source fingerprints in deterministic name order.

Fields outside this projection remain in the immutable raw artifact. The
parser emits bounded `unsupported_fields` warnings with a JSON pointer and
field names so later processing cannot mistake partial interpretation for full
support. Warning and field lists report truncation explicitly when configured
limits are reached. Warning field names are still scanner-controlled evidence
and must not be copied into logs or unsanitized errors.

Changing the meaning or shape of this projection requires a new parser version.
Reprocessing retained evidence under a newer version is replay, not a new scan.

## Uncertainty and scanner outcomes

An `invocation.executionSuccessful: false` value is scanner evidence and does
not make parsing fail. Missing invocations remain missing; the parser does not
invent a successful scan result.

SARIF does not, by itself, establish Open ASPM's authoritative scan scope or
completeness. Parser success therefore never implies `full` coverage. Scope and
completeness remain `unknown` until a later versioned mapping has sufficient
source evidence under ADR-0001.

## Defensive boundaries

Callers construct the parser with explicit finite limits. The parser enforces:

- raw byte count before publication of parsed output;
- UTF-8 and a single JSON object;
- JSON nesting, object-member, array-element, and string limits;
- duplicate-key rejection;
- run, rule, result, invocation, location, fingerprint, and warning limits;
- context cancellation; and
- hard ceilings on configured limits.

Parsing performs no process execution and no network access. Message text,
Markdown, URIs, paths, and other scanner values remain untrusted data. Parser
errors contain only stable codes and parser-generated JSON pointers, never
report excerpts or scanner-controlled values.

The current implementation materializes at most `MaxBytes` in memory. Worker
concurrency must therefore be bounded independently when parser orchestration
is added.

## Error classification

Parser failures expose stable safe codes:

```text
invalid_configuration
input_read_failed
input_too_large
invalid_utf8
malformed_json
unsupported_version
invalid_sarif
limit_exceeded
cancelled
```

These codes classify format processing. They must not replace scanner-reported
failure state retained in parsed invocations.
