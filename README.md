# Open ASPM

Open-source Application Security Posture Management platform for aggregating,
correlating, prioritizing, and tracking security findings across the SDLC.

> [!WARNING]
> Open ASPM is in pre-alpha development and is not ready for production use.
> Interfaces, data models, and deployment procedures may change without notice.

## Why Open ASPM?

Application security findings are fragmented across SAST, SCA, DAST, secret
scanning, container, cloud, and infrastructure security tools. Open ASPM aims
to provide a common place to correlate, prioritize, assign, and track those
findings without replacing the scanners that discover them.

## Planned capabilities

- Scanner, source control, and CI/CD integrations
- Application, repository, artifact, and environment inventory
- Finding normalization, correlation, and deduplication
- Risk-based prioritization and ownership
- Policy and Security Gate evaluation
- Suppression, exception, and risk acceptance workflows
- Audit trail, reporting, API, and CLI

## Non-goals

- Replacing source security scanners
- Automatically changing application code without review
- Claiming compliance based solely on imported findings
- Hiding scanner evidence or silently changing source severity

## Project status

The architecture and domain model are under active design. The initial roadmap
is tracked in [ROADMAP.md](ROADMAP.md). Public milestones and implementation
issues will be added as the interfaces stabilize.

## Documentation

Architecture, threat model, integration contracts, and API documentation will
be versioned in the repository alongside the implementation.

- [Architecture overview](docs/architecture/overview.md)
- [Contributor tasks](CONTRIBUTOR_TASKS.md)

## Contributing

Contributions and design feedback are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md) before
opening a pull request. By participating, you agree to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

New contributors can select a bounded task from
[CONTRIBUTOR_TASKS.md](CONTRIBUTOR_TASKS.md).

## Security

Do not disclose suspected vulnerabilities in public issues. Follow the private
reporting instructions in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
