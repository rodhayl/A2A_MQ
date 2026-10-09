# Security reporting

This experimental project has no security certification or response-time guarantee.
See [the security model](docs/SECURITY.md) for trust boundaries and residual risks.

Do not put credentials, private keys, real user payloads or exploit data in a public
issue. If GitHub offers **Security → Report a vulnerability** for this repository,
use that private channel. Its availability has not been assumed or enabled by this
change. Otherwise contact the owner through an already established private channel;
do not publish sensitive details while arranging disclosure.

A useful report includes the affected commit/version, deployment profile, minimal
synthetic reproduction and impact. State which credentials/scopes the caller needed.
Do not test against infrastructure you do not own or have permission to assess.

Generated Local-lite/Workflow profiles contain secrets and are not shareable evidence.
If a real credential was ever committed, deleting its current file is insufficient:
coordinate revocation/rotation and history remediation with its owner.
