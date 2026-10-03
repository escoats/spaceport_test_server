"""Deterministic snapshot-based trading; no transport or background tasks."""
from __future__ import annotations

from typing import Any

from .demo_server import PROTOCOL_VERSION, RESOURCES


class BotController:
    def __init__(self, world: Any, station_ids: Any) -> None:
        self.world = world
        self.station_ids = tuple(key for key in world.stations if key in station_ids)
        self.sequence = 0
        self.rotation = dict.fromkeys(self.station_ids, 0)

    async def ready(self) -> None:
        for station_id in self.station_ids:
            message = self.world.pb.ClientMessage()
            message.ready.type = self.world.pb.READY_TYPE_READY
            message.ready.protocol_version = PROTOCOL_VERSION
            message.ready.run_id = self.world.run_id
            message.ready.ready = True
            message.ready.snapshot_sequence = 1
            await self.world.handle(station_id, message)

    def command(self, kind: str) -> Any:
        self.sequence += 1
        message = self.world.pb.ClientMessage()
        command = getattr(message, kind)
        command.type = getattr(self.world.pb, {
            'advertise': 'ADVERTISE_TYPE_ADVERTISE', 'accept': 'ACCEPT_TYPE_ACCEPT',
            'offer': 'OFFER_COMMAND_TYPE_OFFER',
        }[kind])
        command.protocol_version = PROTOCOL_VERSION
        command.run_id = self.world.run_id
        command.request_id = f'bot-{self.sequence}'
        return message

    async def step(self) -> None:
        if self.world.phase != 'RUNNING':
            return
        for station_id in self.station_ids:
            state = self.world._state(station_id).state
            message = self.choose(state)
            if message is not None:
                assert message.IsInitialized()
                await self.world.handle(station_id, message)

    def choose(self, state: Any) -> Any:
        pb = self.world.pb
        if state.phase != pb.PHASE_RUNNING or state.self.failed_once or state.self.health == 0:
            return None
        specialty = RESOURCES[state.self.specialty - 1]
        inventory = self.world._proto_bundle(state.self.inventory)
        upkeep = self.world._proto_bundle(state.self.upkeep_per_tick)
        needed = [r for r in RESOURCES if r != specialty and upkeep[r] > 0
                  and inventory[r] < 20 * upkeep[r]]
        for offer in state.offers.items:
            give, pay = self.world._proto_bundle(offer.give), self.world._proto_bundle(offer.receive)
            if (offer.recipient_id == state.self_station_id and offer.status == pb.OFFER_STATUS_OPEN
                and offer.expires_tick > state.tick and any(give[r] > 0 for r in needed)
                and pay[specialty] > 0 and all(pay[r] == 0 for r in RESOURCES if r != specialty)
                and sum(give.values()) >= sum(pay.values())
                and all(inventory[r] - pay[r] >= 10 * upkeep[r] for r in RESOURCES if pay[r])):
                message = self.command('accept')
                message.accept.body.offer_id = offer.offer_id
                return message
        if not any(ad.station_id == state.self_station_id and ad.expires_tick > state.tick
                   for ad in state.advertisements.items):
            message = self.command('advertise')
            message.advertise.body.selling.items.append(state.self.specialty)
            message.advertise.body.seeking.SetInParent()
            message.advertise.body.seeking.items.extend(
                getattr(pb, f'RESOURCE_{r.upper()}') for r in RESOURCES
                if r != specialty and upkeep[r] > 0)
            message.advertise.body.expires_tick = min(state.tick + state.rules.max_publication_ttl_ticks,
                                                      state.rules.duration_ticks)
            return message
        if sum(o.proposer_id == state.self_station_id and o.status == pb.OFFER_STATUS_OPEN
               for o in state.offers.items) >= state.rules.max_open_outgoing_offers:
            return None
        candidates = []
        for peer in state.directory.items:
            if peer.station_id == state.self_station_id or not self.world.stations[peer.station_id].ready:
                continue
            for resource in needed:
                enum = getattr(pb, f'RESOURCE_{resource.upper()}')
                if any(ad.station_id == peer.station_id and enum in ad.selling.items
                       and ad.expires_tick > state.tick for ad in state.advertisements.items):
                    candidates.append((peer.station_id, resource))
        if not candidates:
            return None
        index = self.rotation[state.self_station_id] % len(candidates)
        peer, resource = candidates[index]
        amount = min(5, 20 * upkeep[resource] - inventory[resource],
                     inventory[specialty] - 10 * upkeep[specialty])
        if amount <= 0:
            return None
        self.rotation[state.self_station_id] += 1
        message = self.command('offer')
        message.offer.body.recipient_id = peer
        self.world._copy_bundle(dict.fromkeys(RESOURCES, 0), message.offer.body.give)
        self.world._copy_bundle(dict.fromkeys(RESOURCES, 0), message.offer.body.receive)
        setattr(message.offer.body.give, specialty, amount)
        setattr(message.offer.body.receive, resource, amount)
        message.offer.body.expires_tick = min(state.tick + state.rules.max_offer_ttl_ticks,
                                              state.rules.duration_ticks)
        return message
