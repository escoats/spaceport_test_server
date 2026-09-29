# Spaceport Bazaar Test Server

A deterministic local WebSocket server for exercising Spaceport Bazaar clients.
It speaks the Bazaar v2 binary Protobuf protocol, issues temporary per-station
bearer tokens, waits for every station to become ready, and simulates production,
upkeep, health, offers, acceptances, withdrawals, expiry, and completion.

## Start

```sh
python -m pip install -e '.[dev]'
make generate
spaceport-demo-server --scenario scenarios/default.json \
  --credential-file ./demo-credentials.json
```

The server listens on `ws://127.0.0.1:3001/ws` by default. Give each client the
generated credentials file and the `bazaar.protobuf.v2` WebSocket subprotocol.

## Configure a scenario

`scenarios/default.json` controls the duration, tick speed, upkeep, production,
health behavior, offer limits, and every station's identifier, specialty, and
opening inventory. Copy it to create a repeatable test case. All resource
bundles must name only `water`, `food`, and `components`, with non-negative
integer amounts.

This is a test server rather than a full production Bazaar implementation. It
does not yet implement advertisements, request-result history/idempotency, or
the authoritative server's complete rate-limit and error semantics.
