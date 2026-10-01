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

Use the client URL printed in the console after running `make run`. Try the
"same environment" URL first; if it fails, use the "other containers" URL.
Copy the URL from the current server output, since it can change between runs.

Configure the client to use the `bazaar.protobuf.v2` WebSocket subprotocol.
No authentication is required. Each client receives the first available planet
in scenario order and must declare readiness before participating.

See the [README](README.md) for more configuration and protocol details.
