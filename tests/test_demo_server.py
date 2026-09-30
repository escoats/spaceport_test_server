import asyncio
import json
from contextlib import suppress
from dataclasses import replace
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from spaceport_test_server.demo_server import (
    DeliveryLimits, DemoBazaar, DemoBazaarServer, SUBPROTOCOL, Scenario, run, supervise_ticks,
)


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
    await world.close()


class FakeSocket:
    """Event-controlled transport; no reliance on OS buffers to stall a writer."""

    def __init__(self, *, blocked=False, failure=False, stuck_close=False):
        self.gate = asyncio.Event()
        if not blocked:
            self.gate.set()
        self.entered = asyncio.Event()
        self.close_started = asyncio.Event()
        self.sent = asyncio.Queue()
        self.incoming = asyncio.Queue()
        self.failure = failure
        self.stuck_close = stuck_close
        self.closed = False
        self.close_code = None
        self.cancelled_after_close = False
        self.subprotocol = SUBPROTOCOL

    async def send(self, payload):
        self.entered.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled_after_close = self.close_started.is_set()
            raise
        if self.failure or self.closed:
            raise OSError("test transport failure")
        await self.sent.put(payload)

    async def close(self, code=1000, reason=""):
        self.close_code = code
        self.close_started.set()
        if self.stuck_close:
            await asyncio.Future()
        self.closed = True
        self.gate.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.incoming.get()


@pytest.fixture
async def worlds(tmp_path):
    created = []

    def create(**limits):
        scenario = replace(Scenario.from_file(scenario_file(tmp_path)), duration_ticks=8, tick_duration_ms=10)
        world = DemoBazaar(scenario, delivery_limits=DeliveryLimits(**limits))
        created.append(world)
        return world

    yield create
    for world in created:
        await asyncio.wait_for(world.close(), 1)
        assert not world.connections
        assert not world._session_tasks


async def received(world, socket):
    raw = await asyncio.wait_for(socket.sent.get(), 1)
    message = world.pb.ServerMessage.FromString(raw)
    assert message.IsInitialized()
    return message


def ready_message(world):
    message = world.pb.ClientMessage()
    message.ready.type = world.pb.READY_TYPE_READY
    message.ready.protocol_version = "2.0"
    message.ready.run_id = world.run_id
    message.ready.ready = True
    message.ready.snapshot_sequence = 1
    return message


def sync_message(world):
    message = world.pb.ClientMessage()
    message.sync.type = world.pb.SYNC_TYPE_SYNC
    message.sync.protocol_version = "2.0"
    message.sync.run_id = world.run_id
    return message


async def test_blocked_sender_does_not_delay_ticks_or_healthy_client(worlds):
    world = worlds(max_pending_messages=1)
    slow, healthy = FakeSocket(blocked=True), FakeSocket()
    slow_session = await world.attach("P01", slow)
    await world.attach("P02", healthy)
    await asyncio.wait_for(slow.entered.wait(), 1)
    await received(world, healthy)
    world.phase = "RUNNING"
    for tick in range(1, 4):
        await world.advance_tick()
        await world.broadcast_state()
        assert (await received(world, healthy)).state.tick == tick
    assert not slow_session.closed
    assert len(slow_session.pending) == 1
    slow.gate.set()
    original = (await received(world, slow)).state
    latest = (await received(world, slow)).state
    assert (original.tick, original.snapshot_sequence) == (0, 1)
    assert (latest.tick, latest.snapshot_sequence) == (3, 2)
    assert latest.world_version > original.world_version


async def test_snapshot_coalescing_preserves_results_control_messages_and_captured_state(worlds):
    world = worlds()
    slow = FakeSocket(blocked=True)
    session = await world.attach("P01", slow)
    await asyncio.wait_for(slow.entered.wait(), 1)
    world.phase = "RUNNING"
    await world.advance_tick()
    await world.broadcast_state()
    await world.advance_tick()
    await world.broadcast_state()
    await world._result("P01", "r1", True, "RESULT_CODE_OK")
    await world.advance_tick()
    await world.broadcast_state()
    await world.advance_tick()
    await world.broadcast_state()
    await world.handle("P01", ready_message(world), session=session)
    await world.broadcast_state()
    await world._protocol_error("P01")
    await world.broadcast_state()
    await world._result("P01", "r2", False, "RESULT_CODE_NOT_FOUND")
    # Changes after enqueue must not leak into captured snapshots.
    await world.advance_tick()
    slow.gate.set()
    messages = [await received(world, slow) for _ in range(9)]
    assert [m.WhichOneof("message") for m in messages] == [
        "state", "state", "result", "state", "readiness", "state", "protocol_error", "state", "result"
    ]
    states = [m.state for m in messages if m.WhichOneof("message") == "state"]
    assert [s.tick for s in states] == [0, 2, 4, 4, 4]
    assert [s.snapshot_sequence for s in states] == [1, 2, 3, 4, 5]
    assert [m.result.request_id for m in messages if m.WhichOneof("message") == "result"] == ["r1", "r2"]


@pytest.mark.parametrize("limit", ["count", "bytes"])
async def test_queue_overflow_closes_only_affected_connection(worlds, limit):
    # The initial state fits; many retained results eventually exhaust either cap.
    limits = {"max_pending_messages": 2} if limit == "count" else {"max_pending_bytes": 1024}
    world = worlds(**limits)
    slow, healthy = FakeSocket(blocked=True), FakeSocket()
    session = await world.attach("P01", slow)
    await world.attach("P02", healthy)
    await asyncio.wait_for(slow.entered.wait(), 1)
    await received(world, healthy)
    for index in range(64):
        await world._result("P01", f"request-{index}", True, "RESULT_CODE_OK")
        if session.closed:
            break
    assert session.closed
    await asyncio.wait_for(session.cleanup_task, 1)
    assert slow.close_code == 1013
    assert session.pending_bytes == 0
    assert not session.pending
    assert "P01" not in world.connections
    await world.broadcast_state()
    assert (await received(world, healthy)).state.self_station_id == "P02"


async def test_snapshot_replacement_releases_byte_budget(worlds):
    world = worlds()
    slow = FakeSocket(blocked=True)
    session = await world.attach("P01", slow)
    await asyncio.wait_for(slow.entered.wait(), 1)
    await world.broadcast_state()
    queued_size = session.pending_bytes
    world.delivery_limits = DeliveryLimits(max_pending_bytes=queued_size)
    await world.broadcast_state()
    assert not session.closed
    assert session.pending_bytes == queued_size
    assert len(session.pending) == 1


async def test_send_timeout_closes_before_cancelling_stuck_send(worlds):
    world = worlds(send_timeout=0.02, close_timeout=0.02)
    slow, healthy = FakeSocket(blocked=True, stuck_close=True), FakeSocket()
    session = await world.attach("P01", slow)
    await world.attach("P02", healthy)
    await received(world, healthy)
    await asyncio.wait_for(slow.close_started.wait(), 1)
    await asyncio.wait_for(session.cleanup_task, 1)
    assert slow.close_code == 1013
    assert slow.cancelled_after_close
    assert session.sender_task.done()
    await world.broadcast_state()
    await received(world, healthy)


async def test_transport_failure_isolated_and_readiness_reset(worlds):
    world = worlds()
    broken, healthy = FakeSocket(failure=True), FakeSocket()
    session = await world.attach("P01", broken)
    world.stations["P01"].ready = True
    await world.attach("P02", healthy)
    await asyncio.wait_for(broken.close_started.wait(), 1)
    await asyncio.wait_for(session.cleanup_task, 1)
    assert not world.stations["P01"].ready
    assert "P01" not in world.connections
    await received(world, healthy)
    world.phase = "RUNNING"
    await world.advance_tick()
    await world.broadcast_state()
    assert (await received(world, healthy)).state.tick == 1


async def test_reconnect_fences_old_session_and_restarts_sequence(worlds):
    world = worlds()
    old_socket, new_socket = FakeSocket(), FakeSocket()
    old = await world.attach("P01", old_socket)
    assert (await received(world, old_socket)).state.snapshot_sequence == 1
    await world.handle("P01", ready_message(world), session=old)
    await received(world, old_socket)
    assert world.stations["P01"].ready
    new = await world.attach("P01", new_socket)
    assert not world.stations["P01"].ready
    assert (await received(world, new_socket)).state.snapshot_sequence == 1
    await world.handle("P01", ready_message(world), session=old)
    assert not world.stations["P01"].ready
    await world.detach("P01", old_socket)
    await asyncio.wait_for(old.cleanup_task, 1)
    assert old_socket.close_code == 4001
    assert world.connections["P01"] is new
    await world.handle("P01", sync_message(world), session=new)
    assert (await received(world, new_socket)).state.snapshot_sequence == 2
    await world.handle("P01", ready_message(world), session=new)
    assert (await received(world, new_socket)).readiness.ready
    await world.detach("P01", old_socket)
    assert world.stations["P01"].ready


@pytest.mark.parametrize("late_raw", ["invalid text", b"\xff"])
async def test_old_handler_cannot_send_protocol_errors_to_replacement(worlds, late_raw):
    world = worlds()
    old_socket = FakeSocket()
    old_socket.request = SimpleNamespace(headers={"Authorization": f"Bearer {world.stations['P01'].token}"})
    handler = asyncio.create_task(DemoBazaarServer(world).handler(old_socket))
    try:
        await received(world, old_socket)
        replacement = FakeSocket()
        session = await world.attach("P01", replacement)
        old_socket.incoming.put_nowait(late_raw)
        await asyncio.wait_for(handler, 1)
        assert (await received(world, replacement)).state.snapshot_sequence == 1
        assert replacement.sent.empty()
        assert not session.pending
        assert world.connections["P01"] is session
    finally:
        handler.cancel()
        await asyncio.gather(handler, return_exceptions=True)


async def test_attachment_failure_still_cleans_up_session(worlds, monkeypatch):
    world = worlds()
    socket = FakeSocket()
    socket.request = SimpleNamespace(headers={"Authorization": f"Bearer {world.stations['P01'].token}"})

    def broken_state(station_id):
        raise RuntimeError("snapshot failed")

    monkeypatch.setattr(world, "_state", broken_state)
    with pytest.raises(RuntimeError, match="snapshot failed"):
        await DemoBazaarServer(world).handler(socket)
    assert "P01" not in world.connections
    await world.close()
    assert not world._session_tasks


async def test_tick_failure_is_reported_and_propagated(worlds, monkeypatch, caplog):
    world = worlds()

    async def broken_ticks():
        raise RuntimeError("tick failed")

    monkeypatch.setattr(world, "run_ticks", broken_ticks)
    with pytest.raises(RuntimeError, match="tick failed"):
        await supervise_ticks(world)
    assert any(record.exc_info and "World tick task failed" in record.message for record in caplog.records)
    assert not any(task.get_name() == "world-ticks" for task in asyncio.all_tasks())


async def test_run_shuts_down_after_tick_failure(worlds, monkeypatch, tmp_path):
    world = worlds()
    import spaceport_test_server.demo_server as module

    async def broken_ticks():
        raise RuntimeError("tick failed")

    monkeypatch.setattr(world, "run_ticks", broken_ticks)
    monkeypatch.setattr(module, "DemoBazaar", lambda scenario: world)
    with pytest.raises(RuntimeError, match="tick failed"):
        await run("127.0.0.1", 0, tmp_path / "credentials.json", world.scenario)
    assert world._closing
    assert not world._session_tasks


async def test_two_clients_disconnect_ticks_finish_and_final_state_remains_queryable(worlds):
    world = worlds()
    supervisor = asyncio.create_task(supervise_ticks(world))
    try:
        async with serve(DemoBazaarServer(world).handler, "127.0.0.1", 0, subprotocols=[SUBPROTOCOL], close_timeout=0.1) as server:
            port = server.sockets[0].getsockname()[1]

            def client(station):
                return connect(f"ws://127.0.0.1:{port}/ws", additional_headers={
                    "Authorization": f"Bearer {world.stations[station].token}"}, subprotocols=[SUBPROTOCOL])

            async with client("P01") as first, client("P02") as second:
                await asyncio.wait_for(first.recv(), 1)
                await asyncio.wait_for(second.recv(), 1)
                await first.send(ready_message(world).SerializeToString())
                await second.send(ready_message(world).SerializeToString())
                while True:
                    message = world.pb.ServerMessage.FromString(await asyncio.wait_for(second.recv(), 1))
                    if message.WhichOneof("message") == "state" and message.state.phase == world.pb.PHASE_RUNNING:
                        break
                await first.close()
                sequences, ticks = [], []
                while True:
                    message = world.pb.ServerMessage.FromString(await asyncio.wait_for(second.recv(), 1))
                    if message.WhichOneof("message") == "state":
                        sequences.append(message.state.snapshot_sequence)
                        ticks.append(message.state.tick)
                        if message.state.phase == world.pb.PHASE_FINISHED:
                            break
                assert ticks[-1] == world.scenario.duration_ticks
                assert sequences == list(range(sequences[0], sequences[-1] + 1))
                assert not supervisor.done()
                await second.send(sync_message(world).SerializeToString())
                final = world.pb.ServerMessage.FromString(await asyncio.wait_for(second.recv(), 1)).state
                assert final.phase == world.pb.PHASE_FINISHED
                assert final.snapshot_sequence == sequences[-1] + 1
                assert final.outcome.HasField("value")
    finally:
        supervisor.cancel()
        with suppress(asyncio.CancelledError):
            await supervisor


async def test_shutdown_cleans_blocked_senders_and_is_idempotent(worlds):
    world = worlds(close_timeout=0.02)
    slow = FakeSocket(blocked=True, stuck_close=True)
    session = await world.attach("P01", slow)
    await asyncio.wait_for(slow.entered.wait(), 1)
    await world.broadcast_state()
    await world.close()
    await world.close()
    assert slow.cancelled_after_close
    assert session.sender_task.done()
    assert session.cleanup_task.done()
    assert not world.connections
    assert not world._session_tasks


async def test_offer_result_precedes_snapshot_despite_coalescing(worlds):
    world = worlds()
    socket = FakeSocket(blocked=True)
    session = await world.attach("P01", socket)
    await asyncio.wait_for(socket.entered.wait(), 1)
    world.phase = "RUNNING"
    world.stations["P01"].ready = True
    command = world.pb.ClientMessage()
    offer = command.offer
    offer.type = world.pb.OFFER_COMMAND_TYPE_OFFER
    offer.protocol_version = "2.0"
    offer.run_id = world.run_id
    offer.request_id = "offer-1"
    offer.body.recipient_id = "P02"
    offer.body.give.water = 1
    offer.body.give.food = offer.body.give.components = 0
    offer.body.receive.water = offer.body.receive.food = offer.body.receive.components = 0
    offer.body.expires_tick = 3
    await world.handle("P01", command, session=session)
    await world.advance_tick()
    await world.broadcast_state()
    socket.gate.set()
    initial, result, state = [await received(world, socket) for _ in range(3)]
    assert initial.state.snapshot_sequence == 1
    assert result.result.ok
    assert result.result.request_id == "offer-1"
    assert state.state.snapshot_sequence == 2
    assert state.state.tick == 1
    assert state.state.offers.items[0].offer_id == result.result.object_id.value


async def test_unexpected_close_failure_does_not_leak_tasks(worlds, caplog):
    world = worlds()

    class BrokenCloseSocket(FakeSocket):
        async def close(self, code=1000, reason=""):
            raise RuntimeError("sensitive peer data")

    socket = BrokenCloseSocket(blocked=True)
    session = await world.attach("P01", socket)
    await asyncio.wait_for(socket.entered.wait(), 1)
    await world.close()
    assert session.sender_task.done()
    assert session.cleanup_task.exception() is None
    assert not world._session_tasks
    assert "close failed (RuntimeError)" in caplog.text
    assert "sensitive peer data" not in caplog.text


def advertise_message(world, *, selling=(1,), seeking=(2,), expires=3, request_id='advertise-1'):
    message = world.pb.ClientMessage()
    command = message.advertise
    command.type = world.pb.ADVERTISE_TYPE_ADVERTISE
    command.protocol_version = '2.0'
    command.run_id = world.run_id
    command.request_id = request_id
    command.body.selling.SetInParent()
    command.body.selling.items.extend(selling)
    command.body.seeking.SetInParent()
    command.body.seeking.items.extend(seeking)
    command.body.expires_tick = expires
    return message


async def advertisement_client(world, station='P01'):
    socket = FakeSocket()
    await world.attach(station, socket)
    await received(world, socket)
    world.phase = 'RUNNING'
    world.stations[station].ready = True
    return socket


async def test_advertisements_public_replace_and_leave_inventory_unchanged(worlds):
    world = worlds()
    first = await advertisement_client(world)
    second = await advertisement_client(world, 'P02')
    inventory = {key: dict(value.inventory) for key, value in world.stations.items()}
    await world.handle('P01', advertise_message(world))
    result = (await received(world, first)).result
    state = (await received(world, first)).state
    peer = (await received(world, second)).state
    assert result.ok
    assert state.advertisements == peer.advertisements
    listing = state.advertisements.items[0]
    assert listing.advertisement_id == result.object_id.value
    assert listing.station_id == 'P01'
    assert list(listing.selling.items) == [world.pb.RESOURCE_WATER]
    assert list(listing.seeking.items) == [world.pb.RESOURCE_FOOD]
    assert listing.created_tick == 0
    assert listing.created_version == state.world_version
    assert listing.expires_tick == 3
    await world.handle('P02', advertise_message(world, selling=(), seeking=(3,)))
    await received(world, second)
    await received(world, second)
    await received(world, first)
    await world.handle('P01', advertise_message(world, selling=(3,), seeking=(), request_id='replacement'))
    replacement = (await received(world, first)).result
    state = (await received(world, first)).state
    assert replacement.ok
    assert replacement.object_id.value != result.object_id.value
    assert len(state.advertisements.items) == 2
    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_REPLACED
    assert {key: value.inventory for key, value in world.stations.items()} == inventory


@pytest.mark.parametrize('selling,seeking,expires', [
    ((), (), 3), ((1, 1), (2,), 3), ((1,), (2, 2), 3),
    ((1,), (1,), 3), ((1,), (2,), 0), ((1,), (2,), 4),
])
async def test_invalid_advertisement_does_not_replace_listing(worlds, selling, seeking, expires):
    world = worlds()
    socket = await advertisement_client(world)
    await world.handle('P01', advertise_message(world))
    await received(world, socket)
    await received(world, socket)
    version = world.world_version
    await world.handle('P01', advertise_message(world, selling=selling, seeking=seeking, expires=expires, request_id='invalid'))
    result = (await received(world, socket)).result
    assert not result.ok
    assert result.code == world.pb.RESULT_CODE_INVALID_ARGUMENT
    assert world.world_version == version
    assert len(world.advertisements) == 1
    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_ACTIVE


@pytest.mark.parametrize('invalid', ['ready', 'phase', 'failed', 'run_id', 'protocol_version'])
async def test_advertisement_requires_current_ready_trading_session(worlds, invalid):
    world = worlds()
    socket = await advertisement_client(world)
    message = advertise_message(world)
    if invalid == 'ready':
        world.stations['P01'].ready = False
    elif invalid == 'phase':
        world.phase = 'FINISHED'
    elif invalid == 'failed':
        world.stations['P01'].failed_once = True
    else:
        setattr(message.advertise, invalid, 'invalid')
    await world.handle('P01', message)
    response = await received(world, socket)
    if invalid in {'ready', 'run_id', 'protocol_version'}:
        assert response.protocol_error.code == world.pb.CONTROL_CODE_BAD_MESSAGE
        assert not world.request_history['P01']
    else:
        assert not response.result.ok
    assert not world.advertisements


async def test_advertisement_withdrawal_requires_owner_and_active_listing(worlds):
    world = worlds()
    owner = await advertisement_client(world)
    other = await advertisement_client(world, 'P02')
    await world.handle('P01', advertise_message(world))
    result = (await received(world, owner)).result
    await received(world, owner)
    await received(world, other)
    message = world.pb.ClientMessage()
    command = message.withdraw
    command.type = world.pb.WITHDRAW_TYPE_WITHDRAW
    command.protocol_version = '2.0'
    command.run_id = world.run_id
    command.request_id = 'withdraw-1'
    command.body.object_id = result.object_id.value
    version = world.world_version
    await world.handle('P02', message)
    assert not (await received(world, other)).result.ok
    assert world.world_version == version
    assert len(world._state('P02').state.advertisements.items) == 1
    await world.handle('P01', message)
    assert (await received(world, owner)).result.ok
    assert not (await received(world, owner)).state.advertisements.items
    assert not (await received(world, other)).state.advertisements.items
    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_WITHDRAWN
    message.withdraw.request_id = 'withdraw-again'
    await world.handle('P01', message)
    assert not (await received(world, owner)).result.ok


@pytest.mark.parametrize('finishing', [False, True])
async def test_advertisements_expire_or_close_at_run_end(worlds, finishing):
    world = worlds()
    socket = await advertisement_client(world)
    if finishing:
        world.scenario = replace(world.scenario, duration_ticks=2)
    await world.handle('P01', advertise_message(world, expires=2))
    await received(world, socket)
    await received(world, socket)
    await world.advance_tick()
    assert len(world._state('P01').state.advertisements.items) == 1
    await world.advance_tick()
    await world.broadcast_state()
    assert not (await received(world, socket)).state.advertisements.items
    expected = world.pb.PUBLICATION_STATUS_RUN_ENDED if finishing else world.pb.PUBLICATION_STATUS_EXPIRED
    assert world.advertisements[0].status == expected


@pytest.mark.parametrize('limit', [0, -1, True, 1.5])
def test_invalid_publication_ttl_configuration(tmp_path, limit):
    path = scenario_file(tmp_path)
    raw = json.loads(path.read_text())
    raw['economy']['max_publication_ttl_ticks'] = limit
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match='positive integers'):
        Scenario.from_file(path)


async def test_configured_publication_ttl_and_run_boundary(worlds, tmp_path):
    world = worlds()
    path = scenario_file(tmp_path)
    raw = json.loads(path.read_text())
    raw['economy']['max_publication_ttl_ticks'] = 5
    raw['duration_ticks'] = 4
    path.write_text(json.dumps(raw))
    world.scenario = Scenario.from_file(path)
    socket = await advertisement_client(world)
    assert world._state('P01').state.rules.max_publication_ttl_ticks == 5
    await world.handle('P01', advertise_message(world, expires=5))
    assert not (await received(world, socket)).result.ok
    await world.handle('P01', advertise_message(world, expires=4, request_id='valid-expiry'))
    assert (await received(world, socket)).result.ok


async def test_repeated_offer_request_replays_original_result(worlds):
    world = worlds()
    world.scenario = replace(world.scenario, economy=replace(
        world.scenario.economy, max_open_outgoing_offers=2))
    socket = await advertisement_client(world)
    message = world.pb.ClientMessage()
    offer = message.offer
    offer.type = world.pb.OFFER_COMMAND_TYPE_OFFER
    offer.protocol_version = '2.0'
    offer.run_id = world.run_id
    offer.request_id = 'same-request'
    offer.body.recipient_id = 'P02'
    offer.body.give.water = 1
    offer.body.give.food = offer.body.give.components = 0
    offer.body.receive.water = offer.body.receive.food = offer.body.receive.components = 0
    offer.body.expires_tick = 3
    ids = []
    for _ in range(2):
        await world.handle('P01', message)
        result = (await received(world, socket)).result
        assert result.ok
        ids.append(result.object_id.value)
        await received(world, socket)
    assert ids[0] == ids[1]
    assert len(world.offers) == 1


def trade_message(world, kind, *, recipient='P02', object_id=''):
    message = world.pb.ClientMessage()
    command = getattr(message, kind)
    command.type = getattr(world.pb, {
        'offer': 'OFFER_COMMAND_TYPE_OFFER', 'accept': 'ACCEPT_TYPE_ACCEPT',
        'withdraw': 'WITHDRAW_TYPE_WITHDRAW',
    }[kind])
    command.protocol_version = '2.0'
    command.run_id = world.run_id
    command.request_id = f'{kind}-failure-test'
    if kind == 'offer':
        command.body.recipient_id = recipient
        for resource in ('water', 'food', 'components'):
            setattr(command.body.give, resource, int(resource == 'water'))
            setattr(command.body.receive, resource, 0)
        command.body.expires_tick = 3
    elif kind == 'accept':
        command.body.offer_id = object_id
    else:
        command.body.object_id = object_id
    return message


def gameplay_state(world):
    from copy import deepcopy
    return deepcopy((
        world.stations, world.offers, world.advertisements, world.tick,
        world.world_version, world._advertisement_sequence,
        world._offer_sequence, world._transaction_sequence,
    ))


async def assert_failed_without_mutation(world, station_id, socket, message, monkeypatch):
    from unittest.mock import AsyncMock
    broadcast = AsyncMock()
    monkeypatch.setattr(world, 'broadcast_state', broadcast)
    before = gameplay_state(world)
    await world.handle(station_id, message)
    result = (await received(world, socket)).result
    assert not result.ok
    assert result.code == world.pb.RESULT_CODE_STATION_FAILED
    assert result.request_id == getattr(message, message.WhichOneof('message')).request_id
    assert result.processed_tick == world.tick
    assert result.processed_version == world.world_version
    assert result.object_id.WhichOneof('kind') == 'null'
    assert result.transaction_id.WhichOneof('kind') == 'null'
    assert gameplay_state(world) == before
    broadcast.assert_not_awaited()


@pytest.mark.parametrize('health,failed_once', [(0, False), (50, True)])
@pytest.mark.parametrize('action', ['advertise', 'offer', 'accept', 'withdraw_ad', 'withdraw_offer'])
async def test_dead_initiator_rejected(worlds, monkeypatch, health, failed_once, action):
    world = worlds()
    socket = await advertisement_client(world)
    world.stations['P02'].ready = True
    await world.handle('P01', advertise_message(world))
    await received(world, socket)
    await received(world, socket)
    await world.handle('P01', trade_message(world, 'offer'))
    await received(world, socket)
    await received(world, socket)
    world.stations['P02'].inventory['water'] = 5
    await world.handle('P02', trade_message(world, 'offer', recipient='P01'))
    await received(world, socket)
    if action == 'advertise':
        message = advertise_message(world)
    elif action == 'offer':
        message = trade_message(world, 'offer')
    elif action == 'accept':
        message = trade_message(world, 'accept', object_id=world.offers[1].offer_id)
    else:
        object_id = (world.advertisements[0].advertisement_id if action == 'withdraw_ad'
                     else world.offers[0].offer_id)
        message = trade_message(world, 'withdraw', object_id=object_id)
    getattr(message, message.WhichOneof('message')).request_id = 'after-death'
    world.stations['P01'].health = health
    world.stations['P01'].failed_once = failed_once
    await assert_failed_without_mutation(world, 'P01', socket, message, monkeypatch)


@pytest.mark.parametrize('health,failed_once', [(0, False), (50, True)])
@pytest.mark.parametrize('action', ['offer', 'accept'])
async def test_dead_counterparty_rejected(worlds, monkeypatch, health, failed_once, action):
    world = worlds()
    socket = await advertisement_client(world)
    world.stations['P02'].ready = True
    world.stations['P02'].inventory['water'] = 5
    await world.handle('P02', trade_message(world, 'offer', recipient='P01'))
    await received(world, socket)
    world.stations['P02'].health = health
    world.stations['P02'].failed_once = failed_once
    # Death must win over an acceptance's resource check.
    world.stations['P02'].inventory['water'] = 0
    message = trade_message(world, action, object_id=world.offers[0].offer_id)
    await assert_failed_without_mutation(world, 'P01', socket, message, monkeypatch)


async def test_living_trade_and_withdrawal_to_dead_recipient(worlds):
    world = worlds()
    first = await advertisement_client(world)
    second = await advertisement_client(world, 'P02')
    await world.handle('P01', trade_message(world, 'offer'))
    assert (await received(world, first)).result.ok
    await received(world, first)
    await received(world, second)
    await world.handle('P02', trade_message(world, 'accept', object_id=world.offers[0].offer_id))
    assert (await received(world, second)).result.ok
    await received(world, second)
    await received(world, first)
    assert world.offers[0].status == world.pb.OFFER_STATUS_ACCEPTED
    assert world.stations['P02'].imported_total['water'] == 1
    second_offer = trade_message(world, 'offer')
    second_offer.offer.request_id = 'second-offer'
    await world.handle('P01', second_offer)
    await received(world, first)
    await received(world, first)
    await received(world, second)
    world.stations['P02'].failed_once = True
    await world.handle('P01', trade_message(world, 'withdraw', object_id=world.offers[1].offer_id))
    assert (await received(world, first)).result.ok
    assert world.offers[1].status == world.pb.OFFER_STATUS_WITHDRAWN


async def test_tick_death_hides_ads_and_preserves_sync_reconnect_and_failure(worlds):
    world = worlds()
    first = await advertisement_client(world)
    second = await advertisement_client(world, 'P02')
    await world.handle('P01', advertise_message(world))
    await received(world, first)
    await received(world, first)
    assert (await received(world, second)).state.advertisements.items
    station = world.stations['P01']
    station.health = 1
    station.inventory = dict.fromkeys(station.inventory, 0)
    await world.advance_tick()
    assert station.health == 0 and station.failed_once
    assert station.first_failure_tick == 1
    await world.broadcast_state()
    for socket in (first, second):
        assert not (await received(world, socket)).state.advertisements.items
    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_ACTIVE
    await world.handle('P01', sync_message(world))
    assert not (await received(world, first)).state.advertisements.items
    replacement = FakeSocket()
    await world.attach('P01', replacement)
    state = (await received(world, replacement)).state
    assert state.self.failed_once and not state.advertisements.items
    await world.handle('P01', ready_message(world))
    assert (await received(world, replacement)).readiness.ready
    station.inventory = dict.fromkeys(station.inventory, 100)
    inventory = dict(station.inventory)
    await world.advance_tick()
    assert station.health == 0 and station.first_failure_tick == 1
    assert station.inventory == inventory
    await world.advance_tick()
    assert world.advertisements[0].status == world.pb.PUBLICATION_STATUS_EXPIRED


@pytest.mark.parametrize('kind', ['advertise', 'offer', 'accept', 'withdraw'])
async def test_retry_keeps_original_result_and_gameplay_after_world_changes(worlds, kind):
    world = worlds()
    socket = await advertisement_client(world)
    if kind in {'accept', 'withdraw'}:
        proposer = 'P02' if kind == 'accept' else 'P01'
        world.stations[proposer].ready = True
        world.stations[proposer].inventory['water'] = 10
        await world.handle(proposer, trade_message(world, 'offer', recipient='P01' if proposer == 'P02' else 'P02'))
        if proposer == 'P01':
            await received(world, socket)
        await received(world, socket)
        message = trade_message(world, kind, object_id=world.offers[0].offer_id)
    else:
        message = advertise_message(world) if kind == 'advertise' else trade_message(world, kind)
    await world.handle('P01', message)
    original = (await received(world, socket)).result
    assert original.ok
    snapshot = (await received(world, socket)).state
    assert snapshot.request_results.items[-1] == original
    world.tick = 8
    world.phase = 'FINISHED'
    world.stations['P01'].failed_once = True
    before = gameplay_state(world)
    await world.handle('P01', message)
    assert (await received(world, socket)).result == original
    assert (await received(world, socket)).state.tick == 8
    assert gameplay_state(world) == before


async def test_failed_result_conflicts_capacity_and_private_history(worlds):
    world = worlds()
    world.scenario = replace(world.scenario, economy=replace(
        world.scenario.economy, max_request_records_per_station=1))
    socket = await advertisement_client(world)
    message = advertise_message(world, expires=0)
    await world.handle('P01', message)
    original = (await received(world, socket)).result
    assert original.code == world.pb.RESULT_CODE_INVALID_ARGUMENT
    assert list(world._state('P01').state.request_results.items) == [original]
    assert not world._state('P02').state.request_results.items
    assert world._state('P01').state.rules.max_request_records_per_station == 1
    for changed in (advertise_message(world), trade_message(world, 'offer')):
        getattr(changed, changed.WhichOneof('message')).request_id = message.advertise.request_id
        await world.handle('P01', changed)
        assert (await received(world, socket)).result.code == world.pb.RESULT_CODE_REQUEST_ID_CONFLICT
    await world.handle('P01', advertise_message(world, request_id='new'))
    error = (await received(world, socket)).protocol_error
    assert error.code == world.pb.CONTROL_CODE_REQUEST_CAPACITY_EXCEEDED
    assert error.request_id.value == 'new'
    assert not error.close_session
    assert not world.advertisements
    assert socket.sent.empty()
    await world.handle('P01', message)
    assert (await received(world, socket)).result == original
    assert list((await received(world, socket)).state.request_results.items) == [original]
    peer = await advertisement_client(world, 'P02')
    await world.handle('P02', message)
    assert (await received(world, peer)).result.code == world.pb.RESULT_CODE_INVALID_ARGUMENT
    assert len(world.request_history['P02']) == 1


async def test_reconnect_recovers_result_and_requires_readiness_before_retry(worlds):
    world = worlds()
    socket = await advertisement_client(world)
    message = advertise_message(world)
    await world.handle('P01', message)
    # Replace the connection without consuming the original delivery.
    replacement = FakeSocket()
    await world.attach('P01', replacement)
    state = (await received(world, replacement)).state
    assert state.snapshot_sequence == 1
    original = state.request_results.items[0]
    await world.handle('P01', message)
    assert (await received(world, replacement)).protocol_error.code == world.pb.CONTROL_CODE_BAD_MESSAGE
    await world.handle('P01', ready_message(world))
    await received(world, replacement)
    await world.handle('P01', message)
    assert (await received(world, replacement)).result == original
    await received(world, replacement)
    assert len(world.advertisements) == 1
    await world.handle('P01', sync_message(world))
    assert list((await received(world, replacement)).state.request_results.items) == [original]
    fresh_world = worlds()
    assert not fresh_world.request_history['P01']


@pytest.mark.parametrize('request_id', ['', 'space here', 'x' * 65, 'é'])
async def test_invalid_request_ids_do_not_consume_history(worlds, request_id):
    world = worlds()
    socket = await advertisement_client(world)
    await world.handle('P01', advertise_message(world, request_id=request_id))
    assert (await received(world, socket)).protocol_error.code == world.pb.CONTROL_CODE_BAD_MESSAGE
    assert not world.request_history['P01']


@pytest.mark.parametrize('limit', [0, -1, True, 1.5])
def test_request_history_capacity_must_be_positive_integer(tmp_path, limit):
    path = scenario_file(tmp_path)
    raw = json.loads(path.read_text())
    raw['economy']['max_request_records_per_station'] = limit
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match='positive integers'):
        Scenario.from_file(path)
