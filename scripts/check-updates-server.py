"""Cross-check a disposable Worker with real HA delivery/storage; JSON on stdin."""

import asyncio
from contextlib import suppress
from copy import deepcopy
import json
import logging
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import MappingProxyType
from uuid import UUID

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_URL
from homeassistant.core import HomeAssistant

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from custom_components.registros_camaras.api import RegistrosClient, UpdatesRejected  # noqa: E402
from custom_components.registros_camaras.const import CONF_CENTRE_ID, DOMAIN  # noqa: E402
from custom_components.registros_camaras.update_store import UpdateStore, identity  # noqa: E402
from custom_components.registros_camaras.updates import Updates  # noqa: E402


def require(condition: bool, check: str) -> None:
    if not condition:
        raise AssertionError(check)


class ObservedClient(RegistrosClient):
    """Observe outcomes, without changing HTTP parsing, receipts or retry behavior."""

    def __init__(self, *args):
        super().__init__(*args)
        self.attempts = []
        self.rejections = []
        self.accepted = []
        self.failed = asyncio.Event()

    async def push_updates(self, batch: dict) -> None:
        self.attempts.append(deepcopy(batch))
        try:
            await super().push_updates(batch)
        except UpdatesRejected as err:
            self.rejections.append((err.command_id, err.reason))
            raise
        except Exception:
            self.failed.set()
            raise
        else:
            self.accepted.append(deepcopy(batch))


class ObservedStore(UpdateStore):
    """Signal when the real sender has exhausted deliverable rows, after writes."""

    def __init__(self, *args):
        super().__init__(*args)
        self.idle = asyncio.Event()

    async def next_batch(self, centre_id: str) -> dict | None:
        batch = await super().next_batch(centre_id)
        if batch is None:
            self.idle.set()
        return batch


async def send_until_idle(hass, entry, client, spool) -> None:
    updates = Updates(hass, entry, client)
    updates.spool = spool
    spool.idle.clear()
    updates.send_ready.set()
    sender = asyncio.create_task(updates.send_loop())
    idle = asyncio.create_task(spool.idle.wait())
    failure = asyncio.create_task(client.failed.wait())
    try:
        done, _ = await asyncio.wait(
            (sender, idle, failure), timeout=40, return_when=asyncio.FIRST_COMPLETED
        )
        require(idle in done and not client.failed.is_set(), "sender_did_not_drain")
        require(updates.error is None, "sender_error")
    finally:
        updates.stopping = True
        for task in (sender, idle, failure):
            task.cancel()
        await asyncio.gather(sender, idle, failure, return_exceptions=True)


async def restore(hass, entry) -> ObservedStore:
    spool = ObservedStore(hass, entry.entry_id)
    await spool.initialize()
    require(spool.store.version == 2, "store_v2")
    return spool


def verify_blocked(spool, rejected) -> None:
    descriptors = spool.data.get("blocked", [])
    require(len(descriptors) == 1, "one_blocked_batch")
    require(descriptors[0]["commandId"] == rejected["commandId"], "blocked_command_identity")
    require(descriptors[0]["reason"] == "assignment_window", "blocked_reason")
    require(spool.blocked_batches[descriptors[0]["key"]] == rejected, "blocked_payload_intact")
    require(
        spool.data["rows"] == [[rejected["source"], row] for row in rejected["readings"]],
        "blocked_rows_intact",
    )
    require(spool.data["batch"] is None, "no_pending_ack_or_retry")


async def check(config: dict) -> dict:
    rejected = deepcopy(config["rejectedBatch"])
    healthy = deepcopy(config["healthyReading"])
    healthy_source = config["healthySource"]
    require(type(rejected["apiVersion"]) is int and rejected["apiVersion"] == 1, "api_version")
    UUID(rejected["commandId"])
    require(1 <= len(rejected["readings"]) <= 100, "batch_size")
    require(len({row["assignmentId"] for row in rejected["readings"]}) == 1, "single_assignment")
    require(
        healthy["assignmentId"] != rejected["readings"][0]["assignmentId"], "healthy_assignment"
    )
    original_ids = {identity(rejected["source"], row) for row in rejected["readings"]}
    require(len(original_ids) == len(rejected["readings"]), "distinct_original_identities")
    require(
        healthy["sourceEventId"] not in {row["sourceEventId"] for row in rejected["readings"]},
        "healthy_event_must_be_new",
    )
    require("source" not in healthy, "healthy_source_is_separate")
    headers = {key.lower(): value for key, value in config["headers"].items()}
    require(headers["authorization"].startswith("Bearer "), "bearer_header")
    token = headers["authorization"][len("Bearer ") :]
    require(bool(token.strip()), "bearer_token")
    entry = ConfigEntry(
        domain=DOMAIN,
        version=2,
        minor_version=1,
        title="Disposable cross-check",
        data={CONF_URL: config["url"], CONF_CENTRE_ID: rejected["centreId"]},
        options={},
        source="user",
        unique_id=None,
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
    )
    temp_root = "/tmp/opencode" if Path("/tmp/opencode").is_dir() else None
    with TemporaryDirectory(prefix="ha-updates-cross-", dir=temp_root) as directory:
        hass = HomeAssistant(directory)
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                client = ObservedClient(session, config["url"], token, rejected["centreId"])
                manifest = await client.sensor_stream()
                if "manifest" in config:
                    require(manifest == config["manifest"], "manifest_matches_caller")
                assignments = {item["id"]: item for item in manifest["assignments"]}
                assignment = assignments[healthy["assignmentId"]]
                require(assignment["source"] == healthy_source, "healthy_source_binding")
                require(assignment["endsAt"] is None, "healthy_assignment_active")
                require(healthy["observedAt"] >= assignment["startsAt"], "healthy_assignment_start")
                # Freeze the caller's current single-assignment command before restart.
                bootstrap = Updates(hass, entry, client)
                bootstrap.spool = ObservedStore(hass, entry.entry_id)
                await bootstrap.prepare()
                spool = bootstrap.spool
                await spool.append([(rejected["source"], row) for row in rejected["readings"]])
                await spool.save({**spool.data, "batch": rejected})
                spool = await restore(hass, entry)
                require(
                    await spool.next_batch(client.centre_id) == rejected, "frozen_after_restart"
                )
                await send_until_idle(hass, entry, client, spool)
                require(client.attempts == [rejected], "only_original_payload_sent")
                require(
                    client.rejections == [(rejected["commandId"], "assignment_window")],
                    "typed_422_rejection",
                )
                require(not client.accepted, "rejected_not_acked")
                spool = await restore(hass, entry)
                verify_blocked(spool, rejected)
                require(await spool.next_batch(client.centre_id) is None, "blocked_not_regenerated")
                await spool.append([(healthy_source, healthy)])
                await send_until_idle(hass, entry, client, spool)
                require(
                    len(client.attempts) == 2 and len(client.accepted) == 1, "healthy_only_sent"
                )
                accepted = client.accepted[0]
                require(accepted["commandId"] != rejected["commandId"], "new_healthy_command")
                require(accepted["readings"] == [healthy], "healthy_payload_unchanged")
                require(accepted["source"] == healthy_source, "healthy_source_unchanged")
                spool = await restore(hass, entry)
                verify_blocked(spool, rejected)
                require(
                    await spool.next_batch(client.centre_id) is None, "ack_durable_after_restart"
                )
                return {
                    "ok": True,
                    "storeVersion": 2,
                    "rejectedAttempts": 1,
                    "blockedBatches": 1,
                    "blockedReports": len(rejected["readings"]),
                    "healthyCommandId": accepted["commandId"],
                    "healthyReportsAcknowledged": 1,
                    "blockedIntactAfterRestart": True,
                    "ackDurableAfterRestart": True,
                }
        finally:
            await hass.async_stop(force=True)


if __name__ == "__main__":
    # Never expose credentials, response bodies or payloads through framework tracebacks/logs.
    logging.disable(logging.CRITICAL)
    try:
        result = asyncio.run(check(json.load(sys.stdin)))
    except Exception as err:
        with suppress(OSError):
            print(json.dumps({"ok": False, "error": type(err).__name__}), file=sys.stderr)
        sys.exit(1)
    print(json.dumps(result))
