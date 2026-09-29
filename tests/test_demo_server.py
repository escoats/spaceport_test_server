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
async def test_authenticated_websocket_client_receives_an_initialized_snapshot(tmp_path):
    world = DemoBazaar(Scenario.from_file(scenario_file(tmp_path)))
    async with serve(DemoBazaarServer(world).handler, "127.0.0.1", 0, subprotocols=[SUBPROTOCOL]) as server:
        port = server.sockets[0].getsockname()[1]
        async with connect(f"ws://127.0.0.1:{port}/ws", additional_headers={
            "Authorization": f"Bearer {world.stations['P01'].token}"}, subprotocols=[SUBPROTOCOL]) as socket:
            message = world.pb.ServerMessage.FromString(await socket.recv())
            assert message.IsInitialized()
            assert message.WhichOneof("message") == "state"
            assert message.state.self_station_id == "P01"
