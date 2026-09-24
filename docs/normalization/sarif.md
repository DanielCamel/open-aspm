# SARIF Observation normalization

The first normalization adapter converts the supported source-oriented SARIF
Observation into a bounded Open ASPM representation. It is deterministic and
does not create Finding identity, effective severity, risk, or lifecycle state.

## Identity and compatibility

```text
normalizer_name:    sarif
normalizer_version: 1
input_parser:       sarif
input_parser_version: 1
```

Normalizer version 1 accepts only parser version 1. Supporting a later parser
version requires an explicit compatibility decision and, when output may
change, a new normalizer version.

One immutable output is keyed by workspace, Observation, normalizer name, and
normalizer version. Exact replay returns the existing output. Different output
under the same key is a deterministic conflict. A later version may coexist
with earlier output and never rewrites it.

## Severity mapping

Source severity remains unchanged on the Observation. Normalized severity is a
separate attributed value:

| SARIF `result.level` | Normalized severity |
| --- | --- |
| `error` | `high` |
| `warning` | `medium` |
| `note` | `low` |
| `none` | `not_applicable` |
| absent | `unknown` |

SARIF defines `none` to mean that severity does not apply, so it is not mapped
to `informational`. The SARIF standard also defines rule-configuration and
`kind`-dependent defaults. Parser version 1 preserves the explicitly supplied
result fields but the Observation does not retain the complete configuration
resolution chain. Normalizer version 1 therefore leaves an absent level
`unknown` instead of reconstructing a value from incomplete inputs.

The normalized severity vocabulary also reserves `informational` and
`critical` for future explicitly versioned adapters; SARIF normalizer version 1
does not emit them.

## Category, rule, and location

Category is always `unknown` in version 1. SARIF result messages and rule names
are not interpreted heuristically as vulnerability categories.

When `source_rule_id` exists, it is retained with `rule_kind = source`. This is
a source-scoped key, not a cross-scanner rule taxonomy. When it is absent, both
rule kind and rule identity remain unknown.

The location projection uses the source location with the lowest explicit
ordinal. If that location contains an artifact URI, version 1 retains:

- the URI exactly as untrusted source data;
- `uriBaseId` when present; and
- the supplied line and column region.

It does not resolve URI bases, access the filesystem or network, canonicalize a
repository path, or infer catalog identity. If the primary source location has
no artifact URI, normalized location remains `unknown`; later locations are not
promoted to hide uncertainty in the primary location.

## Provenance

The immutable relationship is:

```text
raw artifact
  -> parser name/version
  -> Observation with source severity and source fields
  -> normalizer name/version
  -> normalized category, severity, rule, and primary location
```

The normalization fingerprint covers the workspace, Observation, normalizer
identity, and normalized content. Processing time is excluded so worker replay
remains deterministic. The originally recorded normalization time is retained
on replay.

## Deliberately deferred

- rule taxonomy and vulnerability-category mapping;
- path or URI identity across repositories and providers;
- correlation fingerprints and stable Findings;
- effective severity, enrichment, risk, and policy decisions; and
- replay orchestration for applying a new normalizer to historical evidence.

See the [SARIF parser contract](../parsing/sarif.md) and the
[architecture overview](../architecture/overview.md) for the surrounding
boundaries.
