"""Cross-repository test against a disposable Worker. Credentials arrive on stdin."""

import asyncio
import json
from pathlib import Path
import sys

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.registros_camaras.api import Conflict, RegistrosClient  # noqa: E402
from custom_components.registros_camaras.const import DEFAULT_CENTRE  # noqa: E402


async def main():
    config = json.load(sys.stdin)
    headers = {key.lower(): value for key, value in config["headers"].items()}
    token = headers["authorization"].removeprefix("Bearer ")
    async with aiohttp.ClientSession() as session:
        client = RegistrosClient(session, config["url"], token, DEFAULT_CENTRE)
        snapshot = await client.snapshot()
        assert snapshot.ready
        assert len(snapshot.tasks) == config["expectedTasks"]
        assert any(
            sum(o["quantity"] for o in chamber["occupants"]) > 0
            for chamber in snapshot.chambers.values()
        )
        page = await client.request(
            "GET", f"production/{DEFAULT_CENTRE}/lots", params={"limit": 50}
        )
        assert page["items"]
        # Stale commands must be rejected by the actual authority, not silently rebased.
        try:
            await client.command(
                "complete_task",
                {
                    "command_id": "00000000-0000-4000-8000-000000099999",
                    "expected_revision": 0,
                    "task_id": snapshot.tasks[0]["id"],
                    "task_revision": snapshot.tasks[0]["revision"],
                    "result": {
                        "kind": "curing_review",
                        "outcome": "ready",
                        "notes": "Contract test",
                    },
                },
            )
        except Conflict:
            pass
        else:
            raise AssertionError("Stale command was accepted")
    print("Python client ↔ authenticated Worker contract passed")


asyncio.run(main())
