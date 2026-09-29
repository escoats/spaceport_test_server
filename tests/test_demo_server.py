import json

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from spaceport_test_server.demo_server import DemoBazaar, DemoBazaarServer, SUBPROTOCOL, Scenario


def scenario_file(tmp_path):
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps({
        "duration_ticks": 1,
        "tick_duration_ms": 1,
        "economy": {"upkeep": {"water": 2, "food": 1, "components": 1}},
        "stations": [
            {"id": "P01", "specialty": "water", "inventory": {"water": 12, "food": 4, "components": 4}},
            {"id": "P02", "specialty": "food", "inventory": {"water": 0, "food": 8, "components": 4}},
        ],
    }))
    return path


def advertise_message(world, request_id, *, selling, seeking, expires_tick=1):
    message = world.pb.ClientMessage()
    command = message.advertise
    command.type = world.pb.ADVERTISE_TYPE_ADVERTISE
    command.protocol_version = "2.0"
    command.run_id = world.run_id
    command.request_id = request_id
    command.body.selling.items.extend(getattr(world.pb, f"RESOURCE_{name.upper()}") for name in selling)
    command.body.seeking.items.extend(getattr(world.pb, f"RESOURCE_{name.upper()}") for name in seeking)
    command.body.expires_tick = expires_tick
    return message


def test_scenario_loads_and_issues_distinct_station_credentials(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    players = world.credentials()["players"]
    assert [player["station_id"] for player in players] == ["P01", "P02"]
    assert len({player["token"] for player in players}) == 2


@pytest.mark.asyncio
async def test_tick_produces_charges_upkeep_and_finishes(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    world.phase = "RUNNING"
    await world.advance_tick()
    assert world.phase == "FINISHED"
    assert world.stations["P01"].inventory == {"water": 14, "food": 3, "components": 3}
    assert world.stations["P02"].health == 92


@pytest.mark.asyncio
async def test_advertise_publishes_and_replaces_station_listing(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    world.phase = "RUNNING"
    for station in world.stations.values():
        station.ready = True

    await world.handle("P01", advertise_message(
        world, "advertise-1", selling=["water"], seeking=["food"]
    ))
    await world.handle("P01", advertise_message(
        world, "advertise-2", selling=["components"], seeking=["food"]
    ))

    state = world._state("P02").state
    assert len(state.advertisements.items) == 2
    first, second = state.advertisements.items
    assert first.status == world.pb.PUBLICATION_STATUS_REPLACED
    assert list(first.selling.items) == [world.pb.RESOURCE_WATER]
    assert second.status == world.pb.PUBLICATION_STATUS_ACTIVE
    assert list(second.selling.items) == [world.pb.RESOURCE_COMPONENTS]
    assert second.station_id == "P01"


@pytest.mark.asyncio
async def test_advertisement_expires_on_tick(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    world.phase = "RUNNING"
    for station in world.stations.values():
        station.ready = True

    await world.handle("P01", advertise_message(
        world, "advertise-1", selling=["water"], seeking=["food"]
    ))
    await world.advance_tick()

    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_EXPIRED


@pytest.mark.asyncio
async def test_authenticated_websocket_client_receives_an_initialized_snapshot(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    world.phase = "RUNNING"
    world.stations["P01"].ready = True
    async with serve(DemoBazaarServer(world).handler, "127.0.0.1", 0, subprotocols=[SUBPROTOCOL]) as server:
        port = server.sockets[0].getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}/ws", additional_headers={
            "Authorization": f"Bearer {world.stations['P01'].token}"}, subprotocols=[SUBPROTOCOL]) as socket:
            message = world.pb.ServerMessage.FromString(await socket.recv())
            assert message.IsInitialized()
            assert message.WhichOneof("message") == "state"
            assert message.state.self_station_id == "P01"

            await socket.send(advertise_message(
                world, "advertise-1", selling=["water"], seeking=["food"]
            ).SerializeToString())
            result = world.pb.ServerMessage.FromString(await socket.recv())
            snapshot = world.pb.ServerMessage.FromString(await socket.recv())
            assert result.WhichOneof("message") == "result"
            assert result.result.ok
            assert snapshot.WhichOneof("message") == "state"
            assert snapshot.state.advertisements.items[0].station_id == "P01"
