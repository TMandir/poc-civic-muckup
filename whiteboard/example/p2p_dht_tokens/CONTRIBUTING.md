# Contributing

Thanks for helping improve this example. Keep changes focused, explain
behavior changes, and include tests for protocol, persistence, or UI behavior.

## Development setup

Use Go 1.23 or newer. From this directory:

```sh
go mod tidy
gofmt -w ./cmd ./internal
go test ./...
go test -race ./...
go vet ./...
```

The peer integration test creates local libp2p and DHT nodes; it may take
longer than ordinary unit tests. Do not make unit tests depend on public
bootstrap or relay services. For deterministic integration tests, create
loopback peers and explicitly enable the test-only private DHT filters.

## Networking and data changes

- Keep bootstrap discovery separate from Circuit Relay v2. A DHT peer is not
  automatically a relay.
- Do not add a public relay/bootstrap address without verifying its operator,
  protocol support, and current availability.
- Keep peer networking independent from the local database and UI packages.
- Preserve `CompareTokenValues` semantics or update its tests and user-facing
  documentation when behavior changes.
- Add bounded timeouts and explicit errors for new network operations.
- Never treat a discovery tag, peer ID, or encrypted transport as peer
  authorization.
- Replication tests should verify both initial snapshot exchange and live
  updates, while keeping test records synthetic.

## Data and secrets

Do not commit LevelDB data, identity private keys, credentials, or real user
tokens. The repository `.gitignore` excludes local example databases and peer
identity files. If a change requires a new persistent secret, document its
permissions, backup, rotation, and loss behavior.

## Licensing

Before redistributing this example outside its parent repository, confirm the
project's license with the repository maintainers. This example does not
declare a separate license.
