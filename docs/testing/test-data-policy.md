# Test Data Policy

## Purpose

Open ASPM processes security reports that may contain source code, credentials,
internal infrastructure details, personal data, and vulnerability evidence.
This policy defines which fixtures may be committed to the public repository
and how they are reviewed.

It applies to unit, integration, fuzzing, performance, UI, and documentation
fixtures in every supported format.

## Core rules

1. Prefer data written specifically for Open ASPM tests.
2. Commit only data whose redistribution rights are understood.
3. Never sanitize customer data for use as a public fixture; create a synthetic
   equivalent instead.
4. Never commit a functional credential, private key, session, cookie, access
   token, webhook secret, or password.
5. Keep ordinary fixtures small, deterministic, and reviewable.
6. Generate dangerous resource-exhaustion inputs at test time under strict
   bounds instead of storing weaponized artifacts.

## Allowed fixture sources

### Synthetic project fixtures

Synthetic fixtures created for Open ASPM are preferred. They should use:

- reserved domains such as `example.com`;
- documentation IP ranges rather than routable private infrastructure;
- obviously fictional organization and repository names;
- minimal code snippets written for the test; and
- nonfunctional credential-shaped placeholders.

New synthetic fixtures are contributed under the repository's Apache-2.0
license unless their directory states another compatible license.

### Upstream standard examples

Examples from a standard or upstream project may be used only when its license
permits redistribution and modification. Preserve required notices and record:

```text
source URL
source revision or release
source file
license SPDX identifier
local modifications
retrieval date
```

Prefer stable release artifacts over copying content from a moving web page.

### Public scanner output

Scanner output from another project is not automatically redistributable merely
because it is publicly accessible. Use it only when both the analyzed input and
the generated output have compatible redistribution terms and no sensitive data.

When uncertain, recreate the minimum structure synthetically.

## Prohibited content

Do not commit:

- customer, employer, or production scanner reports;
- private or proprietary source code;
- real credentials, including revoked credentials;
- real personal data or user identifiers;
- internal hostnames, repository URLs, account IDs, tenant IDs, or cloud
  resource names;
- private vulnerability reports or embargoed advisories;
- exploit payloads that are unnecessary to test parsing behavior;
- malware samples;
- archives designed to exhaust unbounded disk, memory, or CPU; or
- fixtures copied from a source with unknown or incompatible licensing.

Revocation does not make a real credential suitable for Git history. Git
retains deleted content, forks may preserve it, and secret scanners may still
alert on it.

## Synthetic secret-shaped values

Tests of secret finding ingestion need safe values that cannot authenticate.

- Prefer placeholders such as `EXAMPLE_NOT_A_SECRET` when scanner format, type,
  and location are the behavior under test.
- If a provider-shaped value is necessary, use a provider-published test value
  explicitly documented as nonfunctional or construct an intentionally invalid
  value that cannot pass checksum, signature, issuer, or length validation.
- Never derive a fixture by editing one character of a real credential.
- Add a nearby comment or metadata field identifying the value as synthetic.

If repository secret scanning flags an intentional fixture, redesign the
fixture when possible. Any suppression requires maintainer review and a written
reason; push protection must not be broadly disabled.

## Names, paths, URLs, and identities

Use neutral examples:

```text
organization: example-org
repository:   example-service
user:         test-user
domain:       example.com
IPv4:         192.0.2.0/24, 198.51.100.0/24, or 203.0.113.0/24
IPv6:         2001:db8::/32
filesystem:   /workspace/example-service
```

Do not use a real person's name, email, home directory, access identifier, or a
plausible internal domain copied from logs.

## Size and placement

Ordinary committed report fixtures should be no larger than 256 KiB each and
2 MiB per format suite without an approved exception.

Suggested layout:

```text
testdata/
  README.md
  sarif/
    valid/
    invalid/
  cyclonedx/
    valid/
    invalid/
  generated/
```

Larger performance inputs should normally be generated deterministically during
the test or fetched by an explicit benchmark setup that verifies a digest and
license. They must not run in ordinary unit tests.

The `generated/` directory contains generators or compact seeds, not generated
multi-megabyte output committed to Git.

## Malformed and adversarial fixtures

Small malformed inputs are allowed when they demonstrate a parser boundary,
for example:

- invalid JSON tokens;
- unsupported schema versions;
- duplicate identifiers;
- excessive but bounded nesting;
- invalid UTF-8;
- path traversal names inside a tiny archive; or
- markup that must be displayed as text.

The fixture must remain safe to inspect with ordinary development tools.

Do not commit an actual zip bomb or unbounded recursive generator. Tests for
expansion ratio, size, recursion, and timeout limits generate bounded content in
a temporary directory and enforce an absolute maximum before execution.

Fuzzing corpora contain minimal reproductions. Crash artifacts are reviewed and
reduced before being committed.

## Fixture manifest

Each format directory has a `README.md` or machine-readable manifest describing
every nontrivial fixture:

```text
name
purpose
expected result
origin: synthetic | upstream
license
source URL and revision, if upstream
modifications
sensitive-data review date
```

Tests should state whether a fixture is expected to import successfully,
succeed with warnings, or fail with a stable safe error code.

## Review checklist

Every fixture pull request answers:

- [ ] Is the fixture necessary, and is a smaller form sufficient?
- [ ] Is the origin and license documented?
- [ ] Was it created synthetically rather than from customer data?
- [ ] Were credentials, cookies, private keys, personal data, internal URLs,
      repository identifiers, and source paths reviewed?
- [ ] Are all secret-shaped values intentionally nonfunctional?
- [ ] Does the fixture stay within ordinary size limits?
- [ ] Is dangerous content bounded and safe to inspect?
- [ ] Is the expected parser or domain outcome documented and tested?
- [ ] Did repository secret scanning and required CI complete successfully?

Security-related fixture changes receive maintainer review even when the parser
change itself is contributed separately.

## Incident handling

If sensitive content is committed:

1. treat the exposed credential or data as compromised;
2. revoke or rotate credentials before repository cleanup;
3. contact maintainers privately through the security process;
4. remove the content from the active branch;
5. determine whether Git history rewriting is necessary;
6. notify affected parties according to the incident process; and
7. add a regression control without repeating the sensitive value.

Deleting a file in a later commit is not sufficient remediation for a real
secret.
