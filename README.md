# Spaceport Bazaar Test Server

A deterministic local WebSocket server for exercising Spaceport Bazaar clients.
It speaks the Bazaar v2 binary Protobuf protocol, issues temporary per-station
bearer tokens, waits for every station to become ready, and simulates production,
upkeep, health, advertisements, offers, acceptances, withdrawals, expiry, and completion.

## Start

Requires Python 3.11 or newer. Run these commands from the project directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
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
does not yet implement request-result history/idempotency or
the authoritative server's complete rate-limit and error semantics.

## Client delivery behavior

Each connection has an independent outbound queue, so a slow or disconnected
client does not delay world ticks or updates to other clients. Consecutive pending
state snapshots may be replaced by the newest snapshot. Treat each state as a
complete replacement of your local view: intermediate ticks and world versions
may be skipped. Delivered `snapshot_sequence` values remain consecutive and
restart at 1 on a new connection.

Command results, readiness replies, and protocol errors keep their order and are
never coalesced. Snapshots are only replaced when no such message separates them.
A connection is closed with WebSocket code `1013` if a send stalls for five seconds or its pending queue exceeds 64 messages or 8 MiB after snapshot coalescing. Close handshakes are limited to two seconds. Reconnect and declare readiness again to continue; retries are still subject to the lack of idempotency described above.

A new connection replaces the previous connection for that station. Disconnects
reset that station's readiness but do not pause a running simulation. The server
remains available for sync and reconnects after normal completion. Unexpected
tick-task failures are logged and shut down the server instead of leaving a
silently frozen world.

## Advertisements

Send an `advertise` command with `selling.items` and `seeking.items` resource
lists and an absolute `expires_tick`. Either list may be empty, but at least
one resource must be listed. Duplicate resources and resources listed on both
sides are rejected. The station must be ready in a running simulation.

Each station has one active listing. A successful publication replaces its
previous listing, returns the new advertisement ID in `result.object_id`, and
broadcasts the active noticeboard to all connected stations. Listings describe
trading interests; they do not check, reserve, or change inventory.

Set `economy.max_publication_ttl_ticks` in the scenario to control the maximum
lifetime (default: 3 ticks). Expiration must be after the current tick and no
later than either this limit or the run's final tick. Withdraw your own listing
using its ID in `withdraw.body.object_id`. Replaced, withdrawn, expired, and
run-ended listings are omitted from snapshots. At the final tick, remaining
active listings are marked run-ended.

Repeated valid offer submissions still create new offers, even when they reuse
a request ID, subject to the existing offer validation and open-offer limit.
