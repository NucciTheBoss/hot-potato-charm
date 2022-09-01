#!/usr/bin/env python3
# Copyright 2022 Canonical
# See LICENSE file for licensing details.

"""My hot-potato charm!"""

import json
import logging
import random
import time
from typing import Any, List, Union

from ops.charm import (ActionEvent, CharmBase, ConfigChangedEvent,
                       InstallEvent, RelationChangedEvent,
                       RelationDepartedEvent, RelationJoinedEvent)
from ops.framework import StoredState
from ops.main import main
from ops.model import ActiveStatus, Unit


logger = logging.getLogger(__name__)


class Forward:
    @staticmethod
    def forward(
            token: dict, peers: List[str], unit: Unit, **kwargs
        ) -> Union[dict, None]:
        """Recursive function to proceed through hot potato forward table."""

        delay = kwargs.get("delay", 0)
        max_passes = kwargs.get("max_passes", None)
        
        if max_passes is not None:
            if token["times_passed"] == max_passes and token["holder"] == unit.name:
                unit.status = ActiveStatus((
                    'Maximum passes reached. '
                    f'Time to completion is {token["time_elapsed"]:.2f} seconds.'
                ))
                return None

        if token["holder"] != unit.name:
            return None
        else:
            unit.status = ActiveStatus((
                f'M: {token["message"]}, '
                f'H: {token["holder"]}, '
                f'P: {token["times_passed"]}, '
                f'T: {token["time_elapsed"]:.2f}'
            ))
            token["holder"] = peers[random.randint(0, len(peers)-1)]
            token["times_passed"] += 1
            timestamp = time.time()
            token["time_elapsed"] += timestamp - token["timestamp"]
            token["timestamp"] = timestamp
            if delay > 0:
                time.sleep(delay)
            unit.status = ActiveStatus()
            logger.info(f'Current token {token}:')
            # Check if the next destination is the same unit.
            if token["holder"] == unit.name:
                # If so, run forward again
                return Forward.forward(token, peers, unit, delay=delay, max_passes=max_passes)
            else:
                # Return new token
                return token


class _Codec:
    def dumps(self, m: Any) -> str:
        return json.dumps(m)

    def loads(self, m: str) -> Any:
        return json.loads(m)


class HotPotatoCharm(CharmBase, _Codec):

    _stored = StoredState()
    _PASSES_KEY = "passes"
    _DELAY_KEY = "delay"

    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.framework.observe(
            self.on.install,
            self._on_install
        )
        self.framework.observe(
            self.on.config_changed, 
            self._on_config_changed
        )
        self.framework.observe(
            self.on.players_relation_joined, 
            self._on_players_relation_join
        )
        self.framework.observe(
            self.on.players_relation_departed, 
            self._on_players_relation_departed
        )
        self.framework.observe(
            self.on.players_relation_changed,
            self._on_players_relation_changed
        )
        self.framework.observe(
            self.on.start_action, 
            self._on_start_action
        )
        self._stored.set_default(
            bucket={
                self._PASSES_KEY: None,
                self._DELAY_KEY: 0
            }
        )

    def _on_install(self, event: InstallEvent) -> None:
        self.unit.status = ActiveStatus()

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        max_passes = self.config.get("max-passes")
        delay = self.config.get("delay")
        storage = self._stored.bucket

        if max_passes != storage[self._PASSES_KEY]:
            logger.info(f"Updating max passes to {max_passes}.")
            storage.update({self._PASSES_KEY: max_passes})
        if delay != storage[self._DELAY_KEY]:
            logger.info(f"Updating total delay to {delay}.")
            storage.update({self._DELAY_KEY: delay})

    def _on_players_relation_join(self, event: RelationJoinedEvent) -> None:
        """When a new player enters the game."""
        logger.info(f"{self.unit.name}: Hello {event.unit.name}!")

    def _on_players_relation_departed(self, event: RelationDepartedEvent) -> None:
        """When a player leaves the game."""
        logger.info(f"{self.unit.name}: Goodbye {event.unit.name}!")

    def _on_players_relation_changed(self, event: RelationChangedEvent) -> None:
        """When data in players has changed."""
        if "token" not in event.relation.data[event.unit] or event.relation.data[event.unit].get("token") == "":
            logger.info("Key 'token' is not present or is empty in message.")
            return
        else:
            token = self.loads(event.relation.data[event.unit].get("token"))
            r = self.model.relations.get("players")[0]
            peers = [self.unit.name] + [u.name for u in r.units]
            delay = self._stored.bucket[self._DELAY_KEY]
            max_passes = self._stored.bucket[self._PASSES_KEY]

            token = Forward.forward(token, peers, self.unit, delay=delay, max_passes=max_passes)
            if token is None:
                # Do nothing
                return
            else:
                event.relation.data[self.unit].update(
                    {
                        "token": self.dumps(token)
                    }
                )

    def _on_start_action(self, event: ActionEvent) -> None:
        """Handler for when start action is invoked."""
        logger.info("Constructing message and mapping peer topology.")
        delay = self._stored.bucket[self._DELAY_KEY]
        max_passes = self._stored.bucket[self._PASSES_KEY]
        r = self.model.relations.get("players")[0]
        # r.data[self.unit].update({"token": ""})
        peers = [self.unit.name] + [u.name for u in r.units]
        token = {
            "message": event.params["token"],
            "holder": peers[random.randint(0, len(peers)-1)],
            "times_passed": 0,
            "time_elapsed": 0,
            "timestamp": time.time()
        }
        tmp_token = Forward.forward(
            token, peers, self.unit, delay=delay, max_passes=max_passes
        )
        r.data[self.unit].update(
            {
                "token": self.dumps(tmp_token if tmp_token is not None else token) 
            }
        )


if __name__ == "__main__":
    main(HotPotatoCharm)
