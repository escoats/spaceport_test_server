import asyncio
import logging
from dataclasses import replace
from pathlib import Path

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from spaceport_test_server.bots import BotController
from spaceport_test_server.demo_server import DemoBazaar, DemoBazaarServer, Scenario, SUBPROTOCOL
from test_demo_server import FakeSocket, ready_message, received, sync_message, advertise_message


def world(**kwargs):
    return DemoBazaar(Scenario.from_file(Path('scenarios/cooperative-120.json')), **kwargs)


@pytest.mark.parametrize('ids', [('missing',), ('P02', 'P02'), ('P01', 'P02', 'P03')])
def test_invalid_configuration(ids):
    with pytest.raises(ValueError):
        world(bot_stations=ids)


async def test_reservation_readiness_and_reconnect():
    w = world(bot_stations=('P03', 'P02'))
    w.minimum_ready_stations = 1
    await w.bots.ready()
    assert w.phase == 'READY'
    assert w.bots.station_ids == ('P02', 'P03')
    with pytest.raises(ValueError, match='reserved'):
        await w.attach('P02', FakeSocket())
    socket = FakeSocket()
    session = await w.attach('P01', socket)
    await received(w, socket)
    await w.handle('P01', ready_message(w), session=session)
    assert w.phase == 'RUNNING'
    await w.detach('P01', socket)
    assert not w.stations['P01'].ready
    assert w.stations['P02'].ready
    new = FakeSocket()
    await w.attach('P01', new)
    assert (await received(w, new)).state.self_station_id == 'P01'
    assert w.bot_stations == {'P02', 'P03'}
    await w.close()


async def simulate(cooperative):
    w = world(bot_stations=('P02', 'P03'))
    await w.bots.ready()
    await w.handle('P01', ready_message(w))
    reference = BotController(w, ('P01',))
    for _ in range(120):
        if cooperative:
            await reference.step()
        await w.advance_tick()
    return w


async def test_cooperative_survival_and_passive_failure():
    w = await simulate(True)
    assert all(s.health > 0 and not s.failed_once for s in w.stations.values())
    assert w._transaction_sequence > 0
    assert w._state('P01').state.outcome.value.collective_success.value
    for station_id in w.bot_stations:
        commands = [w.pb.ClientMessage.FromString(record[0]) for record in w.request_history[station_id].values()]
        assert all(m.IsInitialized() for m in commands)
        ticks = [w.pb.ServerMessage.FromString(record[1]).result.processed_tick for record in w.request_history[station_id].values()]
        assert len(ticks) == len(set(ticks))
        assert len(commands) == len({getattr(m, m.WhichOneof('message')).request_id for m in commands})
    passive = await simulate(False)
    assert not passive._state('P01').state.outcome.value.collective_success.value
    await w.close()
    await passive.close()


async def test_cooperative_websocket_workflow(caplog):
    caplog.set_level(logging.DEBUG, logger="spaceport_test_server.demo_server")
    w = world(bot_stations=('P02', 'P03'))
    w.scenario = replace(w.scenario, tick_duration_ms=10)
    task = asyncio.create_task(w.run_ticks())
    reference = BotController(w, ('P01',))
    try:
        async with serve(DemoBazaarServer(w).handler, '127.0.0.1', 0, subprotocols=[SUBPROTOCOL]) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f'ws://127.0.0.1:{port}/ws', subprotocols=[SUBPROTOCOL]) as socket:
                initial = w.pb.ServerMessage.FromString(await socket.recv()).state
                assert initial.self_station_id == 'P01'
                assert initial.phase == w.pb.PHASE_READY
                assert initial.tick == 0
                assert initial.run_id.startswith('demo-')
                assert not initial.advertisements.items
                assert not w._ready_to_run.is_set()
                await socket.send(ready_message(w).SerializeToString())
                last_tick = -1
                while True:
                    message = w.pb.ServerMessage.FromString(await asyncio.wait_for(socket.recv(), 3))
                    assert message.IsInitialized()
                    if message.WhichOneof('message') != 'state':
                        continue
                    state = message.state
                    if state.phase == w.pb.PHASE_FINISHED:
                        assert state.outcome.value.collective_success.value
                        break
                    if state.phase == w.pb.PHASE_RUNNING and state.tick != last_tick:
                        last_tick = state.tick
                        command = reference.choose(state)
                        if command:
                            await socket.send(command.SerializeToString())
                await socket.send(sync_message(w).SerializeToString())
                final = w.pb.ServerMessage.FromString(await socket.recv()).state
                assert final.tick == 120
                assert final.transactions.items
                assert all(a.advertisement_id.startswith('demo-advertisement-')
                           for a in w.advertisements)
                assert f'Run {w.run_id} station P01 connected' in caplog.text
                assert f'Run {w.run_id} station P01 ready=True (3/3 required)' in caplog.text
                assert f'Run {w.run_id} started' in caplog.text
                assert 'Collective success: True' in caplog.text
                assert '] AD demo-advertisement-' in caplog.text
                assert '] OFFER demo-offer-' in caplog.text
                assert '] TRADE demo-transaction-' in caplog.text
                assert all(s.health > 0 and not s.failed_once for s in w.stations.values())
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await w.close()


async def test_renewal_limits_expiry_and_reserves():
    w = world(bot_stations=('P02',))
    await w.bots.ready()
    w.phase = 'RUNNING'
    w.stations['P01'].ready = True
    await w.handle('P01', advertise_message(w, selling=(1,), seeking=(2,)))
    for _ in range(8):
        await w.advance_tick()
    ads = [a for a in w.advertisements if a.station_id == 'P02']
    assert [a.created_tick for a in ads] == [0, 3, 6]
    state = w._state('P02').state
    state.self.inventory.water = 0
    state.self.inventory.food = 10
    assert w.bots.choose(state) is None
    state.self.inventory.food = 100
    # Refresh a peer advertisement, then verify equal-unit proposals and cap.
    await w.handle('P01', advertise_message(w, selling=(1,), seeking=(2,), expires=11, request_id='fresh'))
    state = w._state('P02').state
    state.self.inventory.water = 0
    command = w.bots.choose(state)
    assert command.offer.body.give.food == command.offer.body.receive.water == 5
    await w.handle('P02', command)
    assert w.bots.choose(w._state('P02').state) is None
    await w.close()


@pytest.mark.parametrize('payment,received_units,stock,accept', [
    (5, 5, 15, True), (5, 4, 15, False), (5, 5, 14, False),
])
async def test_acceptance_fairness_and_reserve(payment, received_units, stock, accept):
    from test_demo_server import trade_message
    w = world(bot_stations=('P02',))
    await w.bots.ready()
    w.phase = 'RUNNING'
    w.stations['P01'].ready = True
    w.stations['P02'].inventory.update(food=stock, water=0)
    message = trade_message(w, 'offer')
    message.offer.body.give.water = received_units
    message.offer.body.receive.food = payment
    await w.handle('P01', message)
    command = w.bots.choose(w._state('P02').state)
    assert (command.WhichOneof('message') == 'accept') == accept
    if accept:
        await w.handle('P02', command)
        assert w.stations['P02'].inventory['food'] == 10
        assert w.offers[0].status == w.pb.OFFER_STATUS_ACCEPTED
    await w.close()


async def test_peer_rotation_boundary_and_failed_stop():
    w = world(bot_stations=('P02',))
    await w.bots.ready()
    w.phase = 'RUNNING'
    for station_id, selling in [('P01', (1,)), ('P03', (3,))]:
        w.stations[station_id].ready = True
        await w.handle(station_id, advertise_message(w, selling=selling, seeking=(2,)))
    await w.bots.step()  # Advertise first.
    w.stations['P02'].inventory.update(water=0, components=0)
    state = w._state('P02').state
    first, second = w.bots.choose(state), w.bots.choose(state)
    assert [first.offer.body.recipient_id, second.offer.body.recipient_id] == ['P01', 'P03']
    state.tick = 119
    state.advertisements.Clear()
    command = w.bots.choose(state)
    assert command.advertise.body.expires_tick == 120
    state.self.failed_once = True
    assert w.bots.choose(state) is None
    state.self.failed_once = False
    state.phase = w.pb.PHASE_FINISHED
    assert w.bots.choose(state) is None
    await w.close()


async def test_unexpected_bot_error_propagates(monkeypatch, caplog):
    from spaceport_test_server.demo_server import supervise_ticks
    w = world(bot_stations=('P02', 'P03'))
    w.scenario = replace(w.scenario, tick_duration_ms=1)
    await w.handle('P01', ready_message(w))

    async def broken():
        raise RuntimeError('bot failed')

    monkeypatch.setattr(w.bots, 'step', broken)
    with pytest.raises(RuntimeError, match='bot failed'):
        await supervise_ticks(w)
    assert 'World tick task failed' in caplog.text
    await w.close()
