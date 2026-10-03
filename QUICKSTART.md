# Quickstart

## Start the server

Open the project in its dev container and wait for setup to finish. In the
container terminal, run:

```sh
make run
```

This starts the server with `scenarios/default.json`. Leave the terminal running
while clients connect.

Use `MIN=<num>` to change the minimum number of planets that must connect and
declare readiness before the simulation starts, and `SCENARIO=<scenario-name.json>` to choose
the scenario JSON file to run. You can combine both options:

```sh
make run MIN=3 SCENARIO=surplus-3-planets.json
```

`SCENARIO` filenames are resolved inside `scenarios/`. `MIN` must be between 1
and the number of planets in the selected scenario.

## Configure gameplay

Add a new `.json` file to the `scenarios/` directory. Copy the default scenario
as a starting point:

Edit your new file to customize the simulation:

- Increase `duration_ticks` to run the simulation for more ticks.
- Decrease `tick_duration_ms` to make it go faster.
- Change `economy` to alter gameplay rules: resource upkeep, production per
  tick, maximum health, shortage damage, health recovery, and offer limits.
- Add more planets by adding entries to `stations`. Give each a unique `id`,
  a `specialty` of `water`, `food`, or `components`, and an opening `inventory`
  using those resources with non-negative integer amounts.

Start the server with your scenario:

```sh
make run SCENARIO=my-scenario.json
```

By default, the simulation waits for every configured planet's client to connect
and declare readiness. To start with fewer ready clients, set
`minimum_ready_stations` in your JSON or pass an override:

```sh
make run SCENARIO=my-scenario.json MIN=2
```

The minimum must be between 1 and the number of configured planets. Restart the
server to load changes to your scenario.

## Connect a client

Use the client URL printed in the console after running `make run`.
Use the "same environment" URL only when the client runs in the server's container
or host. In a separate container, `127.0.0.1` refers to that client container;
a successful connection there may reach a different server. Use the "other
containers" URL when the containers share a reachable network, or configure
port forwarding and use its address. Copy the URL from the current server output,
since it can change between runs.

Configure the client to use the `bazaar.protobuf.v2` WebSocket subprotocol.
No authentication is required. Each client receives the first available planet
in scenario order and must declare readiness before participating.

See the [README](README.md) for more configuration and protocol details.

## Test with one client

Start with a single ready human planet and no bots:

```sh
make run MIN=1
```

For built-in trading partners and a balanced 120-tick run:

```sh
make run SCENARIO=cooperative-120.json BOTS="P02 P03"
```

The real client receives P01 (water), declares readiness, advertises water for
sale and food/components as wanted, and accepts or proposes fair equal-unit
exchanges before stocks run out. P02 produces food and P03 produces components.
Cooperative trading can keep every planet alive through tick 120; a passive
client can cause collective failure. Other economies may have insufficient
production for survival.

`BOTS` defaults to empty. The CLI equivalent is repeatable `--bot-station P02
--bot-station P03`. IDs must exist, be unique, and leave at least one human
planet. Bot ownership stays fixed throughout the run; disconnected human planets
remain human-controlled. Clients receive available human planets in scenario
order. Bots count toward `MIN`, but at least one human must declare readiness
before a run with bots starts.

Bots submit at most one trading command before each tick, accepting suitable
incoming offers first, renewing advertisements at expiry next, then proposing
specialty exchanges. They aim for twenty ticks of needed stock, trade at most
five units per proposal, and retain ten ticks of upkeep for resources they pay.
They use the existing protocol, offer limits, publication/offer TTLs, and request
history. The balanced scenario starts each resource at 30 units, produces 4 units
of each planet's specialty per tick, and consumes 1 unit of every resource.

Edit `tick_duration_ms` in the scenario to control speed; its default is 250 ms
(about 30 seconds for 120 ticks). Startup logs identify human and bot assignments.
Final logs report each planet's health and failure history, plus collective
success. Final snapshots remain available for sync and reconnects.

## Diagnose a run stuck at tick 0

The server logs the run ID, human connections, readiness counts, and the run start.
With `BOTS="P02 P03"`, it waits for P01's ready message before starting the clock.
A connected client must send `ready=true` with the run ID from its initial state.
If no `station P01 connected` line appears, check the client's configured URL.
If connected appears but readiness does not, check the client's ready message
and the server's protocol-error logs.

This Python server uses run IDs starting with `demo-` and advertisement IDs
starting with `demo-advertisement-`. The bundled validation binary is a separate,
scripted exercise; it does not run this scenario's clock. A client seeing
`advertisement-2` while this server is waiting is evidence of a different endpoint
or incompatible client decoding. Check the raw state run ID and phase.

Normal INFO logs show connections, readiness, run start, and final outcomes.
Pass `--verbose` to the CLI to also log advertisements, offers, settled trades,
rejected commands, and protocol errors. Advertisements announce intentions; trades
happen only when a station accepts an offer. The client must log incoming state
advertisements, offers, and transactions if you also want them in its console.
