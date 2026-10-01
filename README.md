# Spaceport Bazaar Test Server

A deterministic local WebSocket server for exercising Spaceport Bazaar clients.
It speaks the Bazaar v2 binary Protobuf protocol, assigns stations in connection order, waits for every station to become ready, and simulates production,
upkeep, health, advertisements, offers, acceptances, withdrawals, expiry, and completion.

## Start

Open the project in its dev container and wait for container setup to finish.
Start the server with:

```sh
make run
```

Choose a scenario from `scenarios/` by passing its JSON filename through Make:

```sh
make run SCENARIO=surplus-3-planets.json
```

`SCENARIO` defaults to `default.json` in `scenarios/` and can be combined with
`MIN`, for example `make run SCENARIO=surplus-3-planets.json MIN=3`.
Values containing `/` are used as explicit paths, such as
`SCENARIO=scenarios/surplus-3-planets.json` or `SCENARIO=/tmp/custom.json`.
The equivalent direct command-line option is `--scenario <path>`.

For running outside the dev container, install Python 3.11 or newer and set up
the project first:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
make generate
make run
```

For a temporary startup override, pass the minimum through Make:

```sh
make run MIN=3
```

The equivalent direct command-line option is
`--minimum-ready-stations 3`. The override takes precedence over the scenario
value and is validated against the number of configured stations.

The server listens on port `3001` on all container interfaces. When using the
devcontainer port forwarding, connect from the host at `ws://127.0.0.1:3001/ws`.
Use the `bazaar.protobuf.v2` WebSocket subprotocol. No authentication is required;
incoming credentials are ignored. Clients receive the first available station in
scenario order through the initial snapshot’s `self_station_id`. Connections are
rejected when all stations are occupied. The generated `demo-credentials.json`
file is retained for compatibility and is optional.

## Configure a scenario

`scenarios/default.json` controls the duration, tick speed, upkeep, production,
health behavior, offer limits, and every station's identifier, specialty, and
opening inventory. Copy it to create a repeatable test case. All resource
bundles must name only `water`, `food`, and `components`, with non-negative
integer amounts.

Set the top-level `minimum_ready_stations` value to start after any configured
number of stations have declared readiness. It must be between `1` and the
number of configured stations. If omitted, it defaults to all configured
stations, preserving the normal nine-station startup behavior. Stations that
connect after the simulation starts can join without pausing or restarting it.

This is a test server rather than a full production Bazaar implementation. It
does not yet implement the authoritative server's complete rate-limit and error
semantics.

## Client delivery behavior

Each connection has an independent outbound queue, so a slow or disconnected
client does not delay world ticks or updates to other clients. Consecutive pending
state snapshots may be replaced by the newest snapshot. Treat each state as a
complete replacement of your local view: intermediate ticks and world versions
may be skipped. Delivered `snapshot_sequence` values remain consecutive and
restart at 1 on a new connection.

Command results, readiness replies, and protocol errors keep their order and are
never coalesced. Snapshots are only replaced when no such message separates them.
A connection is closed with WebSocket code `1013` if a send stalls for five seconds or its pending queue exceeds 64 messages or 8 MiB after snapshot coalescing. Close handshakes are limited to two seconds. Reconnect and declare readiness again to continue; retry commands with their original request IDs to recover their results.

Disconnects free the station for the next client and reset its readiness, but do
not pause a running simulation. Reconnecting clients receive the first available
station, which may differ from their previous assignment. The server
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

## Request history and retries

Each station retains command results for the lifetime of the server run. Use a
new request ID for each new `advertise`, `offer`, `accept`, or `withdraw` command.
IDs must contain 1–64 ASCII letters, digits, underscores, or hyphens.

Retrying the same decoded command with its original ID returns the original
result followed by a fresh state, without repeating the action or changing the
world version. The result retains its original tick, version, object ID, and
transaction ID, even after expiry, station failure, or normal run completion.
Changing the command type or payload while reusing an ID returns
`RESULT_CODE_REQUEST_ID_CONFLICT`; the original record remains intact.

Snapshots include only the receiving station's stored results in
`request_results.items`, in insertion order. Both successful commands and
commands rejected by gameplay validation are recorded. Reconnects retain this
history, but require readiness again before commands or retries. Restarting the
server creates a new run with empty history.

Set `economy.max_request_records_per_station` to a positive integer to configure
capacity (default: 10,000 per station per run). At capacity, new requests receive only
`CONTROL_CODE_REQUEST_CAPACITY_EXCEEDED`, including their request ID and
`close_session: false`. They neither execute nor consume a record. Exact retries,
conflict checks, sync, and readiness remain available; records are never evicted.
Invalid request IDs, wrong run/protocol values, and commands before readiness
receive `CONTROL_CODE_BAD_MESSAGE` without consuming history. Conflict responses
also do not replace or add records.

## Permanent planet failure

A planet is permanently dead when its health reaches zero or `failed_once` is
true. Its `advertise`, `offer`, `accept`, and `withdraw` commands return
`RESULT_CODE_STATION_FAILED` without changing gameplay state. Living planets
cannot offer trades to dead planets or accept offers from dead proposers.
Existing offers remain visible until their usual lifecycle closes them; living
proposers can still withdraw their own offers to dead recipients.

Dead planets' advertisements are hidden from all snapshots, including sync and
reconnect snapshots. Stored advertisements retain their usual expiry and
end-of-run lifecycle. Readiness, sync, and reconnect access remain available.
