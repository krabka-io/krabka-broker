# Security Policy

## Report a vulnerability

Send a report to <security@krabka.io>. Do not open a public issue or a pull
request for a vulnerability.

Include the affected crate, the commit or tag you tested, the steps to
reproduce the problem, and the impact you observed. A proof of concept helps,
but a clear description is enough.

A maintainer acknowledges a report within five business days and keeps the
reporter informed until the fix lands. Please give the maintainers reasonable
time to fix the problem before you disclose it in public.

## Supported versions

krabka is greenfield and undeployed. There are no release branches, no
production users, and no persisted state to migrate. The `main` branch is the
only supported version, and every fix lands there. Consumers pin the sibling
crates by git revision, so a fix reaches them when they move their pin.

## Scope

The broker's security boundary is the Kafka wire protocol and the operator
tools. Reports of interest include, but are not limited to:

- Authentication or authorization bypass on any listener, including SASL,
  TLS, ACL, and OPA paths.
- A break-glass, write-freeze, or barrier transition that a broker accepts
  without the signature or the approval the design requires. See
  [`krabka-guard`](crates/guard-cli/README.md) and
  [`krabka-barrier`](crates/barrier-cli/README.md).
- A chain, checkpoint, or WORM manifest that
  [`krabka-audit verify`](crates/audit/README.md) or the WORM verifier accepts
  after tampering.
- Memory safety or a panic that a remote client can trigger. The workspace
  forbids `unsafe`, so a panic reachable from the wire is the failure of
  interest.

Kafka wire-protocol behavior that matches Apache Kafka is not a vulnerability
in krabka, even when the Kafka behavior is unsafe.
