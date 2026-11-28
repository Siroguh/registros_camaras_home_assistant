"""Durability, unchanged reports, and real HA event semantics."""

import asyncio
from datetime import UTC, datetime, timedelta
import hashlib
import math
from unittest.mock import AsyncMock, patch

import pytest

from custom_components.registros_camaras.api import CannotConnect, IncompatibleApi, RegistrosClient
from custom_components.registros_camaras.history import HistoryPage
from custom_components.registros_camaras.update_store import UpdateStore
from custom_components.registros_camaras.updates import Updates, reading
from homeassistant.helpers.storage import Store

_STORE_LOAD = Store._async_load
_STORE_WRITE = Store._async_write_data
_STORE_REMOVE = Store.async_remove


@pytest.fixture
def mock_recorder_before_hass(request):
    if "real_recorder" in request.node.name:
        request.getfixturevalue("recorder_db_url")


@pytest.fixture
def real_storage(hass, tmp_path):
    hass.config.config_dir = str(tmp_path)
    with (
        patch.object(Store, "_async_load", _STORE_LOAD),
        patch.object(Store, "_async_write_data", _STORE_WRITE),
        patch.object(Store, "async_remove", _STORE_REMOVE),
    ):
        yield


ASSIGNMENT = {
    "id": "temperature-1",
    "source": "https://home.example",
    "entityId": "sensor.temperature",
    "locationId": "chamber-1",
    "metric": "temperature",
    "position": 1,
    "startsAt": 0,
    "endsAt": None,
    "historyFrom": 0,
}


def manifest(assignments=None):
    return {
        "apiVersion": 1,
        "revision": 1,
        "maxBatchSize": 100,
        "maxBatchDelayMillis": 2000,
        "assignments": [ASSIGNMENT] if assignments is None else assignments,
    }


def sample(n=0):
    return {
        "assignmentId": ASSIGNMENT["id"],
        "sourceEventId": f"2026-11-28T00:00:00.{n:06d}+00:00:change:"
        + hashlib.sha256(b"20").hexdigest(),
        "observedAt": 1795824000000,
        "mode": "live",
        "value": 20,
        "unit": "°C",
        "status": "valid",
    }


async def test_native_store_retains_equal_reports_and_batch_identity_after_restart(hass):
    spool = UpdateStore(hass, "test")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample(n)) for n in range(102)])
    batch = await spool.next_batch("centre")
    assert len(batch["readings"]) == 100
    assert len({r["sourceEventId"] for r in batch["readings"]}) == 100
    restored = UpdateStore(hass, "test")
    await restored.initialize()
    assert await restored.next_batch("centre") == batch
    with pytest.raises(ValueError):
        await restored.acknowledge("wrong-id")
    assert await restored.size() == 102
    # Simulates a lost response after the server committed: retry the exact original command.
    assert await restored.next_batch("centre") == batch
    await restored.acknowledge(batch["commandId"])
    assert await restored.size() == 2
    assert (await restored.next_batch("centre"))["commandId"] != batch["commandId"]


async def test_reconciliation_cursor_and_samples_commit_together(hass):
    spool = UpdateStore(hass, "test")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample())], ("temperature-1", 50))
    assert await spool.cursor("temperature-1", 0) == 50
    with pytest.raises(ValueError):
        await spool.append(
            [
                (ASSIGNMENT["source"], sample(1)),
                (ASSIGNMENT["source"], {**sample(2), "value": float("nan")}),
            ],
            ("temperature-1", 100),
        )
    assert await spool.size() == 1
    assert await spool.cursor("temperature-1", 0) == 50
    await spool.append(
        [(ASSIGNMENT["source"], {**sample(), "mode": "history"})], ("temperature-1", 40)
    )
    assert await spool.size() == 1
    assert (await spool.next_batch("centre"))["readings"][0]["mode"] == "live"
    assert await spool.cursor("temperature-1", 0) == 50


async def test_native_store_write_failure_cannot_acknowledge_or_advance_cursor(hass, real_storage):
    from homeassistant.exceptions import HomeAssistantError
    from homeassistant.util.file import WriteError

    store = UpdateStore(hass, "failed-write")
    await store.initialize()
    await store.append([(ASSIGNMENT["source"], sample())], ("temperature-1", 50))
    batch = await store.next_batch("centre")
    with patch.object(store.store, "_async_write_data", side_effect=WriteError("disk failed")):
        with pytest.raises(HomeAssistantError, match="not persisted"):
            await store.acknowledge(batch["commandId"])
        with pytest.raises(HomeAssistantError, match="not persisted"):
            await store.append([(ASSIGNMENT["source"], sample(2))], ("temperature-1", 100))
    restored = UpdateStore(hass, "failed-write")
    await restored.initialize()
    assert await restored.size() == 1
    assert await restored.cursor("temperature-1", 0) == 50
    assert await restored.next_batch("centre") == batch


async def test_shutdown_uses_homeassistant_final_write(hass, entry):
    from homeassistant.const import EVENT_HOMEASSISTANT_FINAL_WRITE
    from homeassistant.core import CoreState

    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.pending.append((ASSIGNMENT["source"], sample()))
    hass.set_state(CoreState.stopping)
    await updates.stop()
    hass.set_state(CoreState.final_write)
    hass.bus.async_fire(EVENT_HOMEASSISTANT_FINAL_WRITE)
    await hass.async_block_till_done()
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.size() == 1
    hass.set_state(CoreState.not_running)


@pytest.mark.parametrize("damage", ["corrupt", "missing"])
async def test_missing_index_preserves_segments_for_recovery(hass, real_storage, damage):
    from pathlib import Path

    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "damaged-index")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample())])
    batch = await spool.next_batch("centre")
    index = Path(spool.store.path)
    original = await hass.async_add_executor_job(index.read_bytes)
    segment = Path(spool.make_store(spool.segments[0][0]["key"]).path)
    if damage == "corrupt":
        await hass.async_add_executor_job(index.write_text, "{")
    else:
        await hass.async_add_executor_job(index.unlink)
    for _ in range(2):
        with pytest.raises(HomeAssistantError, match="require recovery"):
            await UpdateStore(hass, "damaged-index").initialize()
        assert await hass.async_add_executor_job(segment.exists)
    await hass.async_add_executor_job(index.write_bytes, original)
    restored = UpdateStore(hass, "damaged-index")
    await restored.initialize()
    assert await restored.next_batch("centre") == batch


async def test_repeated_empty_queues_keep_store_keys_and_cache_writes_bounded(
    hass, entry, raw, real_storage
):
    from homeassistant.helpers.storage import get_internal_store_manager

    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    spool = updates.spool
    snapshot = {
        **raw,
        "tasks": [{**raw["tasks"][0], "id": str(n), "description": "x" * 300} for n in range(1500)],
    }
    await spool.cache(snapshot=snapshot, manifest=manifest())
    manager = get_internal_store_manager(hass)
    before = set(manager._invalidated)
    sizes = []
    original = Store._write_prepared_data

    def measure(store, mode, data):
        sizes.append(len(data))
        original(store, mode, data)

    with patch.object(Store, "_write_prepared_data", measure):
        for n in range(100):
            await spool.append([(ASSIGNMENT["source"], sample(n))])
            batch = await spool.next_batch("centre")
            await spool.acknowledge(batch["commandId"])
        await spool.cache(snapshot=snapshot)
    assert manager._invalidated == before
    assert max(sizes) < 4000
    assert sum(sizes) < 400_000
    assert not spool.segments
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["snapshot"] == snapshot
    assert await restored.size() == 0


async def test_real_ha_changed_and_identical_reports_are_copied_without_coalescing(hass, entry):
    client = AsyncMock()
    updates = Updates(hass, entry, client)
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    try:
        hass.states.async_set("sensor.temperature", "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        hass.states.async_set("sensor.temperature", "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        hass.states.async_set("sensor.temperature", "21", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        assert [r[1]["value"] for r in updates.pending] == [20, 20, 21]
        assert len({r[1]["sourceEventId"] for r in updates.pending}) == 3
        snapshot = list(updates.pending)
        hass.states.async_set("sensor.temperature", "21", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        assert list(updates.pending)[:3] == snapshot
        hass.states.async_set("sensor.unselected", "20")
        await hass.async_block_till_done()
        assert len(updates.pending) == 4
    finally:
        updates.unsubscribe()


async def test_batch_ack_is_verified_before_local_removal():
    client = RegistrosClient(AsyncMock(), "https://registros.example", "key", "centre")
    batch = {"commandId": "batch-id"}
    with patch.object(client, "request", new=AsyncMock(return_value={"commandId": "batch-id"})):
        with pytest.raises(IncompatibleApi):
            await client.push_updates(batch)
    with patch.object(
        client,
        "request",
        new=AsyncMock(
            return_value={
                "commandId": "batch-id",
                "stream": "ReadingTransaction/batch-id",
                "eventId": "ReadingTransaction/batch-id:4",
                "version": 4,
            }
        ),
    ):
        await client.push_updates(batch)


async def test_stream_and_push_use_real_http_contract(aiohttp_client, socket_enabled):
    from aiohttp import web

    payloads = []
    manifest = {
        "apiVersion": 1,
        "revision": 1,
        "assignments": [ASSIGNMENT],
        "maxBatchSize": 100,
        "maxBatchDelayMillis": 2000,
    }

    async def stream(request):
        assert request.headers["Authorization"] == "Bearer test-key"
        return web.json_response(manifest)

    async def push(request):
        payload = await request.json()
        payloads.append(payload)
        command_id = payload["commandId"]
        return web.json_response(
            {
                "commandId": command_id,
                "stream": f"ReadingTransaction/{command_id}",
                "eventId": f"ReadingTransaction/{command_id}:4",
                "version": 4,
            }
        )

    app = web.Application()
    app.router.add_get("/api/environment/streams/centre", stream)
    app.router.add_post("/api/environment/updates", push)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", "centre")
    assert (await client.sensor_stream())["assignments"] == [ASSIGNMENT]
    batch = {
        "apiVersion": 1,
        "commandId": "stable-id",
        "centreId": "centre",
        "source": ASSIGNMENT["source"],
        "readings": [sample(1), sample(2)],
    }
    await client.push_updates(batch)
    await client.push_updates(batch)
    assert payloads == [batch, batch]
    manifest["assignments"] = [ASSIGNMENT, ASSIGNMENT]
    with pytest.raises(IncompatibleApi):
        await client.sensor_stream()


async def test_unchanged_reports_keep_microseconds_and_original_freshness(hass):
    hass.states.async_set("sensor.temperature", "20", {"unit_of_measurement": "°C"})
    state = hass.states.get("sensor.temperature")
    at = datetime(2026, 11, 28, microsecond=1, tzinfo=UTC)
    first = reading(ASSIGNMENT, state, at, "live")
    second = reading(ASSIGNMENT, state, at + timedelta(microseconds=1), "live")
    assert first["observedAt"] == second["observedAt"]
    assert first["sourceEventId"] != second["sourceEventId"]


async def test_unload_flushes_pending_reports_for_restart(hass, entry):
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.pending.append((ASSIGNMENT["source"], sample()))
    await updates.stop()
    assert not updates.pending
    assert await updates.spool.size() == 1


async def test_failed_unload_retries_flush_on_homeassistant_shutdown(
    hass, entry, client, stream_manifest, real_storage
):
    from homeassistant.exceptions import HomeAssistantError

    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    with patch.object(
        updates.spool, "append", side_effect=HomeAssistantError("temporary disk failure")
    ):
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        assert not await hass.config_entries.async_unload(entry.entry_id)
        assert len(updates.pending) == 1
    await hass.async_stop()
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.size() == 1
    assert (await restored.next_batch("centre"))["readings"][0]["value"] == 20


@pytest.mark.parametrize("offline", [False, True])
async def test_missing_cache_recovers_online_without_changing_pending_batch(
    hass, entry, client, stream_manifest, real_storage, offline
):
    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    batch = await updates.spool.next_batch("centre")
    cursors = dict(updates.spool.data["cursors"])
    assert await hass.config_entries.async_unload(entry.entry_id)
    await updates.spool.make_store(updates.spool.cache_key).async_remove()
    if offline:
        client.side_effect = CannotConnect()
        stream_manifest.side_effect = CannotConnect()
        assert not await hass.config_entries.async_setup(entry.entry_id)
        hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        assert await restored.size() == 1
        assert await restored.next_batch("centre") == batch
        client.side_effect = None
        stream_manifest.side_effect = None
        assert await hass.config_entries.async_unload(entry.entry_id)
    assert await hass.config_entries.async_setup(entry.entry_id)
    restored = entry.runtime_data.updates.spool
    assert await restored.next_batch("centre") == batch
    assert restored.data["cursors"] == cursors
    assert "manifest" in restored.data and "snapshot" in restored.data
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_full_queue_reserves_shutdown_space_without_losing_buffer(hass, entry):
    from homeassistant.exceptions import HomeAssistantError

    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    with (
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
        patch("custom_components.registros_camaras.update_store.MEMORY_REPORTS", 2),
    ):
        await updates.spool.append([(ASSIGNMENT["source"], sample(n)) for n in range(2)])
        updates.pending.extend((ASSIGNMENT["source"], sample(n)) for n in range(2, 4))
        with pytest.raises(HomeAssistantError, match="capacity"):
            await updates.spool.append(list(updates.pending))
        await updates.stop()
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        assert await restored.size() == 4
        batch = await restored.next_batch("centre")
        assert [row["sourceEventId"] for row in batch["readings"]] == [
            sample(n)["sourceEventId"] for n in range(4)
        ]
        with pytest.raises(HomeAssistantError, match="capacity"):
            await restored.append([(ASSIGNMENT["source"], sample(4))], shutdown=True)
        await restored.acknowledge(batch["commandId"])
        await restored.append([(ASSIGNMENT["source"], sample(4))])
        assert await restored.size() == 1


async def test_restart_drains_shutdown_reserve_before_resubscribing(
    hass, entry, client, stream_manifest
):
    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    prepared = Updates(hass, entry, AsyncMock())
    await prepared.prepare()
    store = prepared.spool
    await store.append([(ASSIGNMENT["source"], sample(n)) for n in range(3)])
    with patch("custom_components.registros_camaras.updates.MAX_REPORTS", 2):
        assert await hass.config_entries.async_setup(entry.entry_id)
        updates = entry.runtime_data.updates
        assert updates.paused
        assert not updates.unsubscribers
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        assert not updates.pending
        assert await hass.config_entries.async_unload(entry.entry_id)
        assert await updates.spool.size() == 3


async def test_manifest_storage_failure_does_not_kill_reconciliation(hass, entry):
    from homeassistant.exceptions import HomeAssistantError

    updates = Updates(hass, entry, AsyncMock())

    async def wait(_):
        if refresh.await_count == 2:
            updates.stopping = True

    with (
        patch.object(updates, "recover", new=AsyncMock(return_value=False)),
        patch.object(
            updates, "refresh", new=AsyncMock(side_effect=[HomeAssistantError("disk full"), None])
        ) as refresh,
        patch.object(updates, "wait", side_effect=wait),
        patch.object(hass.loop, "time", side_effect=[0, 301, 301, 602, 602]),
    ):
        await updates.reconcile_loop()
    assert refresh.await_count == 2


async def test_native_entry_unload_joins_transport_and_removes_subscriptions(
    hass, entry, client, stream_manifest
):
    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert all(task.done() for task in updates.tasks)
    assert not updates.tasks[0].cancelled()
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.size() == 1
    hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    assert not updates.pending


async def test_sender_retries_identical_batch_after_lost_response(hass, entry):
    client = AsyncMock()
    client.centre_id = "centre"
    updates = Updates(hass, entry, client)
    await updates.spool.initialize()
    await updates.spool.append([(ASSIGNMENT["source"], sample())])
    acknowledged = asyncio.Event()
    original_ack = updates.spool.acknowledge

    async def ack(command_id):
        await original_ack(command_id)
        updates.stopping = True
        acknowledged.set()

    client.push_updates.side_effect = [CannotConnect("response lost"), None]
    updates.send_ready.set()
    with (
        patch.object(updates.spool, "acknowledge", side_effect=ack),
        patch.object(updates, "wait", new=AsyncMock()),
    ):
        task = asyncio.create_task(updates.send_loop())
        try:
            await asyncio.wait_for(acknowledged.wait(), 5)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert client.push_updates.call_count == 2
    assert client.push_updates.call_args_list[0] == client.push_updates.call_args_list[1]
    assert await updates.spool.size() == 0


async def test_recorder_overlap_is_deduplicated_and_cursor_commits_with_rows(hass, entry):
    from homeassistant.core import State

    updates = Updates(hass, entry, AsyncMock())
    updates.assignments = [ASSIGNMENT]
    await updates.spool.initialize()
    at = datetime.now(UTC) - timedelta(seconds=20)
    start = int(at.timestamp() * 1000) - 1000
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], start)
    state = State("sensor.temperature", "20", {"unit_of_measurement": "°C"}, last_updated=at)
    await updates.spool.append([(ASSIGNMENT["source"], reading(ASSIGNMENT, state, at, "live"))])
    hass.config.components.add("recorder")
    recorder = AsyncMock()
    recorder.queue_task = lambda task: task.future.set_result(None)
    recorder.async_add_executor_job.side_effect = lambda work, *args: work(*args)
    with (
        patch("homeassistant.components.recorder.get_instance", return_value=recorder),
        patch(
            "custom_components.registros_camaras.updates.read_page",
            side_effect=lambda _hass, _entity, _start, end: HistoryPage([state], end),
        ) as history,
    ):
        await updates.recover()
    assert history.call_args.args[1] == "sensor.temperature"
    assert history.call_args.args[2] == start
    assert await updates.spool.size() == 1
    assert await updates.spool.cursor(ASSIGNMENT["id"], 0) > start
    batch = await updates.spool.next_batch("centre")
    assert batch["readings"][0]["mode"] == "live"


async def test_coincident_equal_reports_never_collapse(hass, entry):
    from homeassistant.core import Event, State

    updates = Updates(hass, entry, AsyncMock())
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC)
    state = State("sensor.temperature", "20", {"unit_of_measurement": "°C"}, last_updated=at)
    event = Event(
        "state_reported", {"entity_id": state.entity_id, "new_state": state, "last_reported": at}
    )
    updates.capture(event)
    updates.capture(event)
    assert len(updates.pending) == 2
    first, second = [row for _, row in updates.pending]
    assert first["observedAt"] == second["observedAt"]
    assert first["sourceEventId"] != second["sourceEventId"]


@pytest.mark.parametrize(
    "interval,expected_samples,expected_batches",
    [(0, 0, 0), (60, 36, 36), (10, 216, 216), (1, 2160, 1080)],
)
async def test_36_sensor_workload_preserves_every_report_with_bounded_batches(
    hass, interval, expected_samples, expected_batches
):
    spool = UpdateStore(hass, "workload")
    await spool.initialize()
    sent = []
    batches = 0
    for second in range(60):
        if interval and second % interval == 0:
            await spool.append(
                [
                    (ASSIGNMENT["source"], {**sample(second), "assignmentId": f"sensor-{n}"})
                    for n in range(36)
                ]
            )
        if second % 2 == 1:
            while batch := await spool.next_batch("centre"):
                assert len(batch["readings"]) <= 100
                sent.extend(batch["readings"])
                batches += 1
                await spool.acknowledge(batch["commandId"])
    assert len(sent) == expected_samples
    assert batches == expected_batches
    assert await spool.size() == 0


async def test_shutdown_joins_blocked_executor_before_final_snapshot(
    hass, entry, client, stream_manifest, real_storage
):
    import threading

    from homeassistant.core import CoreState
    from homeassistant.helpers.storage import Store

    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    entered = asyncio.Event()
    release = threading.Event()
    original = Store._write_prepared_data
    blocked = False

    def write(store, mode, data):
        nonlocal blocked
        if "/segment." in store.key and not blocked:
            blocked = True
            hass.loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
        original(store, mode, data)

    with patch.object(Store, "_write_prepared_data", write):
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
        await asyncio.wait_for(entered.wait(), 5)
        hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
        hass.set_state(CoreState.running)
        stopping = asyncio.create_task(hass.async_stop())
        try:
            await asyncio.wait_for(updates.stop_ready.wait(), 5)
            assert not stopping.done()
        finally:
            release.set()
        await asyncio.wait_for(stopping, 10)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    batch = await restored.next_batch(entry.data["centre_id"])
    assert [row["value"] for row in batch["readings"]] == [20, 21]
    assert all(task.done() for task in updates.tasks)
    assert not updates.tasks[0].cancelled()


async def test_coincident_changes_and_recorder_overlap_keep_every_occurrence(hass, entry):
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    at = datetime.now(UTC) - timedelta(seconds=20)
    states = []
    try:
        with patch("homeassistant.core.time.time", return_value=at.timestamp()):
            for value in (20, 21, 20):
                hass.states.async_set(
                    ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C"}
                )
                await hass.async_block_till_done()
                states.append(hass.states.get(ASSIGNMENT["entityId"]))
        hass.states.async_set(ASSIGNMENT["entityId"], "22", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        rows = list(updates.pending)
        assert len({r["sourceEventId"] for _, r in rows}) == 4
        assert len({r["observedAt"] for _, r in rows[:3]}) == 1
        await updates.spool.append(rows)
        await updates.spool.ensure_cursor(ASSIGNMENT["id"], int(at.timestamp() * 1000) - 1000)
        hass.config.components.add("recorder")
        recorder = AsyncMock()
        recorder.queue_task = lambda task: task.future.set_result(None)
        recorder.async_add_executor_job.side_effect = lambda work, *args: work(*args)
        with (
            patch("homeassistant.components.recorder.get_instance", return_value=recorder),
            patch(
                "custom_components.registros_camaras.updates.read_page",
                side_effect=lambda _hass, _entity, _start, end: HistoryPage(states, end),
            ),
        ):
            await updates.recover()
        assert await updates.spool.size() == 4
        assert [r["value"] for r in (await updates.spool.next_batch("centre"))["readings"]] == [
            20,
            21,
            20,
            22,
        ]
    finally:
        updates.unsubscribe()


async def test_manifest_removes_last_local_subscription(hass, entry):
    hass.config.external_url = ASSIGNMENT["source"]
    client = AsyncMock()
    updates = Updates(hass, entry, client)
    await updates.prepare()
    client.sensor_stream.return_value = manifest()
    await updates.refresh()
    assert updates.unsubscribers
    client.sensor_stream.return_value = manifest(
        [{**ASSIGNMENT, "source": "https://other.example"}]
    )
    await updates.refresh()
    assert updates.assignments == []
    assert updates.unsubscribers == []
    hass.states.async_set(ASSIGNMENT["entityId"], "20")
    await hass.async_block_till_done()
    assert not updates.pending


async def test_offline_restart_captures_equal_reports_then_delivers(
    hass, entry, client, stream_manifest
):
    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    assert await hass.config_entries.async_unload(entry.entry_id)
    client.side_effect = CannotConnect()
    stream_manifest.side_effect = CannotConnect()
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    assert not entry.runtime_data.last_update_success
    assert updates.unsubscribers
    for _ in range(2):
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
    assert await updates.spool.size() == 2
    client.side_effect = None
    stream_manifest.side_effect = None
    await entry.runtime_data.async_refresh()
    await updates.refresh()
    assert entry.runtime_data.last_update_success
    await updates.stop()
    updates.stopping = False
    delivered = []

    async def push(batch):
        delivered.extend(batch["readings"])
        updates.stopping = True

    with (
        patch.object(updates.client, "push_updates", side_effect=push),
        patch.object(updates, "wait", new=AsyncMock()),
    ):
        updates.send_ready.set()
        await updates.send_loop()
    assert len(delivered) == 2
    assert len({r["sourceEventId"] for r in delivered}) == 2
    assert await updates.spool.size() == 0
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_full_queue_drain_has_bounded_write_volume_and_restarts(
    hass, entry, raw, real_storage
):
    from homeassistant.helpers.storage import Store

    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    spool = updates.spool
    snapshot = {
        **raw,
        "tasks": [{**raw["tasks"][0], "id": str(n), "description": "x" * 300} for n in range(1500)],
    }
    await spool.cache(snapshot=snapshot, manifest=manifest())
    sizes = []
    original = Store._write_prepared_data

    def measure(store, mode, data):
        sizes.append(len(data))
        original(store, mode, data)

    with patch.object(Store, "_write_prepared_data", measure):
        await spool.append([(ASSIGNMENT["source"], sample(n)) for n in range(50000)])
        fill_bytes = sum(sizes)
        sizes.clear()
        count = 0
        while batch := await spool.next_batch("centre"):
            if count == 0:
                spool = UpdateStore(hass, entry.entry_id)
                await spool.initialize()
                assert spool.data["snapshot"] == snapshot
                assert await spool.next_batch("centre") == batch
            count += len(batch["readings"])
            await spool.acknowledge(batch["commandId"])
    assert count == 50000
    assert max(sizes) < 128000
    assert sum(sizes) < 32_000_000
    print(
        f"Full queue: fill={fill_bytes} bytes, drain={sum(sizes)} bytes, "
        f"max commit={max(sizes)} bytes"
    )


async def test_recovery_waits_for_real_recorder_commit(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path
):
    recorder = await async_setup_recorder_instance(
        hass,
        {
            "db_url": f"sqlite:///{tmp_path / 'recorder.db'}",
            "commit_interval": 60,
        },
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC)
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], int(at.timestamp() * 1000) - 1000)
    with patch("homeassistant.core.time.time", return_value=at.timestamp()):
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    await recorder.async_block_till_done()
    with patch(
        "homeassistant.core.time.time", return_value=(at + timedelta(seconds=1)).timestamp()
    ):
        hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    await recorder.async_add_executor_job(recorder.block_till_done)
    assert recorder._event_session_has_pending_writes
    with patch("custom_components.registros_camaras.updates.datetime") as clock:
        clock.now.return_value = at + timedelta(seconds=20)
        clock.fromtimestamp.side_effect = datetime.fromtimestamp
        await updates.recover()
    batch = await updates.spool.next_batch("centre")
    assert [r["value"] for r in batch["readings"]] == [20, 21]
    assert all(r["mode"] == "history" for r in batch["readings"])
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.cursor(ASSIGNMENT["id"], 0) == int(
        (at + timedelta(seconds=10)).timestamp() * 1000
    )


async def test_shutdown_timeout_flushes_ram_with_http_and_executor_blocked(
    hass, entry, client, stream_manifest, real_storage
):
    import threading

    from homeassistant.const import EVENT_HOMEASSISTANT_STOP

    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    http_entered = asyncio.Event()
    writer_entered = asyncio.Event()
    stopped = asyncio.Event()
    release = threading.Event()
    original_write = Store._write_prepared_data
    original_wait = Updates.wait

    async def slow_http(_):
        http_entered.set()
        await asyncio.Event().wait()

    async def wait(updates, seconds):
        await original_wait(updates, 0 if seconds == 2 else seconds)

    def write(store, mode, data):
        if "/segment." in store.key:
            hass.loop.call_soon_threadsafe(writer_entered.set)
            assert release.wait(5)
        original_write(store, mode, data)

    async def on_stop(_):
        stopped.set()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, on_stop)
    with (
        patch.object(RegistrosClient, "push_updates", side_effect=slow_http),
        patch.object(Updates, "wait", wait),
        patch("homeassistant.core.STOPPING_STAGE_SHUTDOWN_TIMEOUT", 0.05),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        hass.states.async_set(ASSIGNMENT["entityId"], "20", {"unit_of_measurement": "°C"})
        await asyncio.wait_for(http_entered.wait(), 5)
        with patch.object(Store, "_write_prepared_data", write):
            hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
            await asyncio.wait_for(writer_entered.wait(), 5)
            hass.states.async_set(ASSIGNMENT["entityId"], "22", {"unit_of_measurement": "°C"})
            stopping = asyncio.create_task(hass.async_stop())
            try:
                await asyncio.wait_for(stopped.wait(), 5)
            finally:
                release.set()
            await asyncio.wait_for(stopping, 10)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert [r["value"] for _, r in restored.data["rows"]] == [20, 21, 22]
    assert (await restored.next_batch(entry.data["centre_id"]))["readings"][0]["value"] == 20


async def test_real_recorder_excluded_attributes_keep_live_identity(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path
):
    recorder = await async_setup_recorder_instance(
        hass,
        {
            "db_url": f"sqlite:///{tmp_path / 'recorder.db'}",
            "commit_interval": 60,
        },
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    at = datetime.now(UTC)
    try:
        with patch("homeassistant.core.time.time", return_value=at.timestamp()):
            for value in (20, 21, 20):
                hass.states.async_set(
                    ASSIGNMENT["entityId"],
                    str(value),
                    {
                        "unit_of_measurement": "°C",
                        "attribution": "Excluded by Recorder",
                    },
                )
                await hass.async_block_till_done()
        await recorder.async_block_till_done()
        await updates.spool.append(list(updates.pending))
        await updates.spool.ensure_cursor(ASSIGNMENT["id"], int(at.timestamp() * 1000) - 1000)
        with patch("custom_components.registros_camaras.updates.datetime") as clock:
            clock.now.return_value = at + timedelta(seconds=20)
            clock.fromtimestamp.side_effect = datetime.fromtimestamp
            await updates.recover()
        assert await updates.spool.size() == 3
        batch = await updates.spool.next_batch("centre")
        assert [r["value"] for r in batch["readings"]] == [20, 21, 20]
        assert all(r["mode"] == "live" for r in batch["readings"])
    finally:
        updates.unsubscribe()


async def test_real_recorder_dequeued_event_waits_for_commit_barrier(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path
):
    import threading

    from homeassistant.const import EVENT_STATE_CHANGED

    recorder = await async_setup_recorder_instance(
        hass,
        {
            "db_url": f"sqlite:///{tmp_path / 'recorder.db'}",
            "commit_interval": 60,
        },
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC)
    start = int(at.timestamp() * 1000) - 1000
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], start)
    await recorder.async_block_till_done()
    entered = asyncio.Event()
    queued = asyncio.Event()
    release = threading.Event()
    original = recorder._process_one_event
    original_queue = recorder.queue_task

    def process(event):
        if (
            event.event_type == EVENT_STATE_CHANGED
            and event.data.get("entity_id") == ASSIGNMENT["entityId"]
        ):
            hass.loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
        original(event)

    def enqueue(task):
        queued.set()
        original_queue(task)

    with patch.object(recorder, "_process_one_event", process):
        hass.states.async_set(ASSIGNMENT["entityId"], "21", {"unit_of_measurement": "°C"})
        await asyncio.wait_for(entered.wait(), 5)
        assert recorder._queue.empty()
        assert not recorder._event_session_has_pending_writes
        with (
            patch.object(recorder, "queue_task", enqueue),
            patch("custom_components.registros_camaras.updates.datetime") as clock,
        ):
            clock.now.return_value = at + timedelta(seconds=20)
            clock.fromtimestamp.side_effect = datetime.fromtimestamp
            recovery = asyncio.create_task(updates.recover())
            try:
                await asyncio.wait_for(queued.wait(), 5)
                assert not recovery.done()
                assert await updates.spool.cursor(ASSIGNMENT["id"], 0) == start
            finally:
                release.set()
            await asyncio.wait_for(recovery, 5)
    batch = await updates.spool.next_batch("centre")
    assert [r["value"] for r in batch["readings"]] == [21]


@pytest.mark.parametrize("history_first", [False, True])
@pytest.mark.parametrize("live_unit", ["°C", ""])
async def test_real_recorder_missing_unit_preserves_live_payload_and_cursor(
    recorder_db_url,
    hass,
    entry,
    async_setup_recorder_instance,
    tmp_path,
    real_storage,
    history_first,
    live_unit,
):
    await async_setup_recorder_instance(
        hass,
        {
            "db_url": f"sqlite:///{tmp_path / 'recorder.db'}",
            "commit_interval": 60,
        },
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    at = datetime.now(UTC)
    try:
        hass.states.async_set(
            ASSIGNMENT["entityId"], "20", {"unit_of_measurement": live_unit, "large": "x" * 17000}
        )
        await hass.async_block_till_done()
        live = list(updates.pending)
        if not history_first:
            await updates.spool.append(live)
            batch = await updates.spool.next_batch("centre")
        await updates.spool.ensure_cursor(ASSIGNMENT["id"], int(at.timestamp() * 1000) - 1000)
        with patch("custom_components.registros_camaras.updates.datetime") as clock:
            clock.now.return_value = at + timedelta(seconds=20)
            clock.fromtimestamp.side_effect = datetime.fromtimestamp
            await updates.recover()
        if history_first:
            assert updates.spool.data["rows"][0][1]["unit"] == ""
            assert await updates.spool.next_batch("centre") is None
            await updates.spool.append(live)
            batch = await updates.spool.next_batch("centre")
        assert await updates.spool.size() == 1
        assert await updates.spool.next_batch("centre") == batch
        assert batch["readings"] == [live[0][1]]
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        assert await restored.next_batch("centre") == batch
        assert restored.data["rows"][0][1] == live[0][1]
        assert await restored.cursor(ASSIGNMENT["id"], 0) == int(
            (at + timedelta(seconds=10)).timestamp() * 1000
        )
        await restored.acknowledge(batch["commandId"])
        reopened = UpdateStore(hass, entry.entry_id)
        await reopened.initialize()
        assert await reopened.next_batch("centre") is None
        assert await reopened.size() == 0
    finally:
        updates.unsubscribe()


async def test_real_recorder_incomplete_reserve_never_pauses_live_capture(
    recorder_db_url,
    hass,
    entry,
    async_setup_recorder_instance,
    tmp_path,
    real_storage,
    client,
    stream_manifest,
):
    await async_setup_recorder_instance(
        hass,
        {
            "db_url": f"sqlite:///{tmp_path / 'recorder.db'}",
            "commit_interval": 60,
        },
    )
    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC)
    for offset, value in [(20, "20"), (19, "21")]:
        with patch(
            "homeassistant.core.time.time",
            return_value=(at - timedelta(seconds=offset)).timestamp(),
        ):
            hass.states.async_set(
                ASSIGNMENT["entityId"], value, {"unit_of_measurement": "°C", "large": "x" * 17000}
            )
            await hass.async_block_till_done()
    await updates.spool.ensure_cursor(
        ASSIGNMENT["id"], int((at - timedelta(seconds=21)).timestamp() * 1000)
    )
    with patch("custom_components.registros_camaras.update_store.MAX_INCOMPLETE_REPORTS", 2):
        await updates.recover()
    cursor = await updates.spool.cursor(ASSIGNMENT["id"], 0)
    assert await updates.spool.size() == 2
    assert await updates.spool.deliverable_size() == 0
    assert await updates.spool.next_batch("centre") is None
    hass.states.async_set(
        ASSIGNMENT["entityId"], "22", {"unit_of_measurement": "°C", "large": "x" * 17000}
    )
    await hass.async_block_till_done()
    with (
        patch(
            "custom_components.registros_camaras.update_store.MAX_INCOMPLETE_REPORTS",
            2,
        ),
        patch("custom_components.registros_camaras.updates.datetime") as clock,
    ):
        clock.now.return_value = at + timedelta(seconds=20)
        clock.fromtimestamp.side_effect = datetime.fromtimestamp
        assert not await updates.recover()
        assert "capacity" in updates.recovery_error
    assert await updates.spool.cursor(ASSIGNMENT["id"], 0) == cursor
    sent = asyncio.Event()
    original_wait = Updates.wait
    original_acknowledge = UpdateStore.acknowledge

    async def wait(instance, seconds):
        await original_wait(instance, 0 if seconds == 2 else seconds)

    async def push(batch):
        assert [row["value"] for row in batch["readings"]] == [23]

    async def acknowledge(store, command_id):
        await original_acknowledge(store, command_id)
        sent.set()

    with (
        patch("custom_components.registros_camaras.updates.MAX_REPORTS", 2),
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
        patch(
            "custom_components.registros_camaras.update_store.MAX_INCOMPLETE_REPORTS",
            2,
        ),
        patch.object(Updates, "wait", wait),
        patch.object(RegistrosClient, "push_updates", side_effect=push),
        patch.object(UpdateStore, "acknowledge", acknowledge),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        restored = entry.runtime_data.updates
        assert not restored.paused
        assert restored.unsubscribers
        hass.states.async_set(ASSIGNMENT["entityId"], "23", {"unit_of_measurement": "°C"})
        await asyncio.wait_for(sent.wait(), 5)
        assert await hass.config_entries.async_unload(entry.entry_id)
    reopened = UpdateStore(hass, entry.entry_id)
    await reopened.initialize()
    assert [row["value"] for _, row in reopened.data["rows"]] == [20, 21]
    assert cursor <= await reopened.cursor(ASSIGNMENT["id"], 0) < int(at.timestamp() * 1000)


async def test_incomplete_history_does_not_block_other_reports_or_weaken_conflicts(
    hass, real_storage
):
    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "missing-unit")
    await spool.initialize()
    await spool.append(
        [
            (ASSIGNMENT["source"], {**sample(), "mode": "history", "unit": ""}),
            (ASSIGNMENT["source"], sample(1)),
        ]
    )
    batch = await spool.next_batch("centre")
    assert batch["readings"] == [sample(1)]
    await spool.acknowledge(batch["commandId"])
    restored = UpdateStore(hass, "missing-unit")
    await restored.initialize()
    assert await restored.size() == 1
    with pytest.raises(HomeAssistantError, match="Conflicting"):
        await restored.append([(ASSIGNMENT["source"], {**sample(), "value": 99})])
    await restored.append([(ASSIGNMENT["source"], sample())])
    batch = await restored.next_batch("centre")
    assert batch["readings"] == [sample()]
    await restored.append([(ASSIGNMENT["source"], {**sample(), "mode": "history", "unit": ""})])
    assert await restored.next_batch("centre") == batch


async def test_real_recorder_incomplete_window_does_not_starve_other_assignments(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, real_storage
):
    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    other = {**ASSIGNMENT, "id": "temperature-2", "entityId": "sensor.other"}
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT, other]
    at = datetime.now(UTC)
    for value in (20, 21):
        hass.states.async_set(
            ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C", "large": "x" * 17000}
        )
        await hass.async_block_till_done()
    hass.states.async_set(other["entityId"], "23", {"unit_of_measurement": "°C"})
    await hass.async_block_till_done()
    initial = int(at.timestamp() * 1000) - 1000
    for assignment in updates.assignments:
        await updates.spool.ensure_cursor(assignment["id"], initial)
    with (
        patch("custom_components.registros_camaras.update_store.MAX_INCOMPLETE_REPORTS", 1),
        patch("custom_components.registros_camaras.updates.datetime") as clock,
    ):
        clock.now.return_value = at + timedelta(seconds=20)
        clock.fromtimestamp.side_effect = datetime.fromtimestamp
        for _ in range(2):
            assert not await updates.recover()
            assert "capacity" in updates.recovery_error
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.cursor(ASSIGNMENT["id"], 0) == initial
    assert await restored.cursor(other["id"], 0) == int(
        (at + timedelta(seconds=10)).timestamp() * 1000
    )
    batch = await restored.next_batch("centre")
    assert [(row["assignmentId"], row["value"]) for row in batch["readings"]] == [(other["id"], 23)]


async def test_definitively_rejected_mapping_retains_batch_and_delivers_healthy_after_restart(
    hass, entry, real_storage
):
    from custom_components.registros_camaras.api import UpdatesRejected

    client = AsyncMock()
    client.centre_id = "centre"
    updates = Updates(hass, entry, client)
    await updates.spool.initialize()
    await updates.spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await updates.spool.next_batch("centre")
    healthy = {**sample(1), "assignmentId": "healthy"}
    await updates.spool.append([(ASSIGNMENT["source"], healthy)])
    attempts = []

    async def push(batch):
        attempts.append(batch)
        if batch == frozen:
            raise UpdatesRejected(batch["commandId"], "assignment_window")
        assert batch["readings"] == [healthy]
        updates.stopping = True

    with (
        patch.object(client, "push_updates", side_effect=push),
        patch.object(updates, "wait", new=AsyncMock()),
    ):
        updates.send_ready.set()
        await updates.send_loop()
    assert len(attempts) == 2
    assert updates.spool.data["rows"] == [[ASSIGNMENT["source"], sample()]]
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.next_batch("centre") is None
    blocked = restored.data["blocked"][0]
    assert blocked["reason"] == "assignment_window"
    assert restored.blocked_batches[blocked["key"]] == frozen
    assert restored.blocked_diagnostics()["blocked_reports"] == 1
    assert "home.example" not in str(restored.blocked_diagnostics())
    with pytest.raises(ValueError, match="acknowledgement"):
        await restored.acknowledge(frozen["commandId"])
    await restored.append([(ASSIGNMENT["source"], healthy)])
    assert (await restored.next_batch("centre"))["readings"] == [healthy]


@pytest.mark.parametrize("failure", ["batch", "index"])
async def test_block_failure_preserves_original_retry_and_restart(hass, real_storage, failure):
    from homeassistant.exceptions import HomeAssistantError
    from homeassistant.util.file import WriteError

    spool = UpdateStore(hass, "failed-block")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await spool.next_batch("centre")
    original = Store._async_write_data

    async def fail(store, data):
        if (failure == "batch" and "/blocked." in store.key) or (
            failure == "index" and store.key == spool.key
        ):
            raise WriteError("disk failed")
        await original(store, data)

    with patch.object(Store, "_async_write_data", fail):
        with pytest.raises(HomeAssistantError, match="not persisted"):
            await spool.block(frozen["commandId"], "assignment_window")
    assert await spool.next_batch("centre") == frozen
    restored = UpdateStore(hass, "failed-block")
    await restored.initialize()
    assert await restored.next_batch("centre") == frozen
    assert restored.blocked_diagnostics()["blocked_batches"] == 0
    await restored.block(frozen["commandId"], "assignment_window")
    assert await restored.next_batch("centre") is None


async def test_blocked_capacity_is_bounded_without_stopping_healthy_queued_delivery(hass):
    from custom_components.registros_camaras.update_store import ReportCapacityFull

    spool = UpdateStore(hass, "blocked-budget")
    await spool.initialize()
    with (
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
        patch("custom_components.registros_camaras.update_store.MEMORY_REPORTS", 1),
    ):
        await spool.append([(ASSIGNMENT["source"], sample())])
        frozen = await spool.next_batch("centre")
        await spool.block(frozen["commandId"], "assignment_window")
        good = {**sample(1), "assignmentId": "healthy"}
        await spool.append([(ASSIGNMENT["source"], good)])
        with pytest.raises(ReportCapacityFull):
            await spool.append([(ASSIGNMENT["source"], sample(2))])
        healthy_batch = await spool.next_batch("centre")
        assert healthy_batch["readings"] == [good]
        await spool.acknowledge(healthy_batch["commandId"])
        await spool.append(
            [(ASSIGNMENT["source"], sample(2)), (ASSIGNMENT["source"], sample(3))], shutdown=True
        )
        with pytest.raises(ReportCapacityFull):
            await spool.append([(ASSIGNMENT["source"], sample(4))], shutdown=True)
        assert await spool.size() == 3
        assert spool.blocked_diagnostics()["blocked_report_capacity"] == 3
        assert spool.blocked_diagnostics()["blocked_reports"] == 1


async def test_real_recorder_tombstone_distinguished_from_empty_unknown_unavailable(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, real_storage
):
    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC)
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], int(at.timestamp() * 1000) - 1000)
    originals = []
    for state in ("20", "", "unknown", "unavailable"):
        hass.states.async_set(ASSIGNMENT["entityId"], state, {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        originals.append(hass.states.get(ASSIGNMENT["entityId"]))
    hass.states.async_remove(ASSIGNMENT["entityId"])
    await hass.async_block_till_done()
    with patch("custom_components.registros_camaras.updates.datetime") as clock:
        clock.now.return_value = at + timedelta(seconds=20)
        await updates.recover()
    batch = await updates.spool.next_batch("centre")
    assert batch["readings"] == [
        reading(ASSIGNMENT, state, state.last_updated, "history") for state in originals
    ]
    assert [r["status"] for r in batch["readings"]] == [
        "valid",
        "invalid",
        "invalid",
        "unavailable",
    ]
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert await restored.next_batch("centre") == batch


async def test_real_recorder_pages_keep_coincident_identities_after_restart(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, real_storage
):
    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    at = datetime.now(UTC).replace(microsecond=0)
    initial = int(at.timestamp() * 1000)
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], initial)
    try:
        # First page must leave the entire repeated-time group for the next page.
        for millis, value in [(0, 19), (1, 20), (1, 21), (1, 20), (2, 22), (3, 23)]:
            with patch(
                "homeassistant.core.time.time",
                return_value=(at + timedelta(milliseconds=millis)).timestamp(),
            ):
                hass.states.async_set(
                    ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C"}
                )
                await hass.async_block_till_done()
        expected = [r for _, r in updates.pending]
    finally:
        updates.unsubscribe()
    updates.pending.clear()
    delivered = []
    with (
        patch("custom_components.registros_camaras.history.PAGE_REPORTS", 3),
        patch("custom_components.registros_camaras.updates.datetime") as clock,
    ):
        clock.now.return_value = at + timedelta(seconds=20)
        for _ in range(4):
            await updates.recover()
            while batch := await updates.spool.next_batch("centre"):
                delivered.extend(batch["readings"])
                await updates.spool.acknowledge(batch["commandId"])
            restored = UpdateStore(hass, entry.entry_id)
            await restored.initialize()
            updates.spool = restored
    assert delivered == [{**row, "mode": "history"} for row in expected]
    assert len({row["sourceEventId"] for row in delivered}) == 6


async def test_real_recorder_overfull_group_does_not_starve_other_assignment(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, real_storage
):
    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    other = {**ASSIGNMENT, "id": "other", "entityId": "sensor.other"}
    updates = Updates(hass, entry, AsyncMock())
    updates.assignments = [ASSIGNMENT, other]
    await updates.spool.initialize()
    at = datetime.now(UTC).replace(microsecond=0)
    initial = int(at.timestamp() * 1000)
    for a in updates.assignments:
        await updates.spool.ensure_cursor(a["id"], initial)
    with patch("homeassistant.core.time.time", return_value=at.timestamp()):
        for value in (20, 21, 20):
            hass.states.async_set(ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C"})
            await hass.async_block_till_done()
        hass.states.async_set(other["entityId"], "25", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
    with (
        patch("custom_components.registros_camaras.history.PAGE_REPORTS", 2),
        patch("custom_components.registros_camaras.updates.datetime") as clock,
    ):
        clock.now.return_value = at + timedelta(seconds=20)
        for _ in range(2):
            await updates.recover()
        assert "Coincident" in updates.recovery_error
    assert await updates.spool.cursor(ASSIGNMENT["id"], 0) == initial
    assert await updates.spool.cursor(other["id"], 0) > initial
    assert [r["assignmentId"] for r in (await updates.spool.next_batch("centre"))["readings"]] == [
        "other"
    ]


async def test_recovery_capacity_adapts_whole_groups_and_isolates_assignments(hass, entry):
    from homeassistant.core import State

    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    other = {**ASSIGNMENT, "id": "other", "entityId": "sensor.other"}
    updates.assignments = [ASSIGNMENT, other]
    at = datetime.now(UTC) - timedelta(seconds=20)
    initial = int(at.timestamp() * 1000)
    for a in updates.assignments:
        await updates.spool.ensure_cursor(a["id"], initial)
    first = [
        State(ASSIGNMENT["entityId"], str(v), {"unit_of_measurement": "°C"}, last_updated=at)
        for v in (20, 21, 20)
    ]
    healthy = State(other["entityId"], "25", {"unit_of_measurement": "°C"}, last_updated=at)
    hass.config.components.add("recorder")
    recorder = AsyncMock()
    recorder.queue_task = lambda task: task.future.set_result(None)
    recorder.async_add_executor_job.side_effect = lambda work, *args: work(*args)
    with (
        patch("homeassistant.components.recorder.get_instance", return_value=recorder),
        patch(
            "custom_components.registros_camaras.updates.read_page",
            side_effect=lambda _h, entity, _s, end: HistoryPage(
                first if entity == ASSIGNMENT["entityId"] else [healthy], end
            ),
        ),
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
    ):
        await updates.recover()
    assert await updates.spool.cursor(ASSIGNMENT["id"], 0) == initial
    assert await updates.spool.cursor(other["id"], 0) > initial
    assert (await updates.spool.next_batch("centre"))["readings"][0]["assignmentId"] == other["id"]


async def test_real_recorder_capacity_adapts_and_resumes_remaining_rows_after_restart(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, real_storage
):
    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.assignments = [ASSIGNMENT]
    at = datetime.now(UTC).replace(microsecond=0)
    initial = int(at.timestamp() * 1000)
    await updates.spool.ensure_cursor(ASSIGNMENT["id"], initial)
    for millis in range(3):
        with patch(
            "homeassistant.core.time.time",
            return_value=(at + timedelta(milliseconds=millis)).timestamp(),
        ):
            hass.states.async_set(
                ASSIGNMENT["entityId"], str(20 + millis), {"unit_of_measurement": "°C"}
            )
            await hass.async_block_till_done()
    delivered = []
    with (
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
        patch("custom_components.registros_camaras.updates.datetime") as clock,
    ):
        clock.now.return_value = at + timedelta(seconds=20)
        assert await updates.recover()
        assert await updates.spool.cursor(ASSIGNMENT["id"], 0) == initial + 1
        batch = await updates.spool.next_batch("centre")
        delivered.extend(batch["readings"])
        await updates.spool.acknowledge(batch["commandId"])
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        updates.spool = restored
        assert not await updates.recover()
        batch = await updates.spool.next_batch("centre")
        delivered.extend(batch["readings"])
        assert [row["value"] for row in delivered] == [20, 21, 22]
        assert len({row["sourceEventId"] for row in delivered}) == 3


async def test_real_storage_snapshot_failure_flushes_latest_on_final_write(
    hass, entry, client, raw, real_storage
):
    from custom_components.registros_camaras.api import decode_snapshot
    from custom_components.registros_camaras.const import DEFAULT_CENTRE
    from homeassistant.const import EVENT_HOMEASSISTANT_FINAL_WRITE
    from homeassistant.core import CoreState
    from homeassistant.util.file import WriteError

    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    original = Store._async_write_data

    async def fail_cache(store, data):
        if "/cache." in store.key:
            raise WriteError("disk failed")
        await original(store, data)

    raw["revision"] = 8
    client.return_value = decode_snapshot(raw, DEFAULT_CENTRE)
    updates.snapshot_next_write = 0
    with patch.object(Store, "_async_write_data", fail_cache):
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert entry.runtime_data.last_update_success
        assert updates.snapshot_pending["revision"] == 8
        assert updates.spool.data["snapshot"]["revision"] == 7
    hass.set_state(CoreState.stopping)
    await updates.stop()
    assert updates.snapshot_timer is None
    hass.set_state(CoreState.final_write)
    hass.bus.async_fire(EVENT_HOMEASSISTANT_FINAL_WRITE)
    await hass.async_block_till_done()
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["snapshot"]["revision"] == 8
    hass.set_state(CoreState.running)
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_real_storage_full_queue_retries_cache_without_starvation(
    hass, entry, client, raw, real_storage
):
    from custom_components.registros_camaras.api import decode_snapshot
    from custom_components.registros_camaras.const import DEFAULT_CENTRE, DOMAIN

    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    await updates.spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await updates.spool.next_batch(DEFAULT_CENTRE)
    await updates.spool.block(frozen["commandId"], "assignment_window")
    pending = (ASSIGNMENT["source"], sample(1))
    updates.pending.append(pending)
    retries = asyncio.Queue()
    resume = asyncio.Event()

    async def retry_handshake():
        await retries.put(30)
        await resume.wait()
        resume.clear()

    with (
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 1),
        patch.object(updates, "wait_for_capacity", side_effect=retry_handshake),
        patch.object(updates.spool, "cache", wraps=updates.spool.cache) as cache,
    ):
        try:
            updates.write_ready.set()
            assert await asyncio.wait_for(retries.get(), 5) == 30
            # A verified command refresh arrives while report persistence is retrying.
            updates.snapshot_next_write = 0
            raw["revision"] = 8
            client.return_value = decode_snapshot(raw, DEFAULT_CENTRE)
            receipt = {"version": 8}
            with patch.object(
                entry.runtime_data.client, "command", new=AsyncMock(return_value=receipt)
            ):
                result = await hass.services.async_call(
                    DOMAIN,
                    "confirm_proposal",
                    {
                        "config_entry_id": entry.entry_id,
                        "command_id": "00000000-0000-4000-8000-000000000123",
                        "expected_revision": 7,
                        "location_id": "cam-1",
                        "proposal_id": "proposal:7",
                    },
                    blocking=True,
                    return_response=True,
                )
            assert result == receipt
            assert updates.snapshot_pending["revision"] == 8
            assert updates.spool.data["snapshot"]["revision"] == 7
            resume.set()
            assert await asyncio.wait_for(retries.get(), 5) == 30
            restored = UpdateStore(hass, entry.entry_id)
            await restored.initialize()
            assert restored.data["snapshot"]["revision"] == 8
            assert updates.snapshot_pending is None
            assert updates.cache_error is None
            assert cache.await_count == 1
            next_write = updates.snapshot_next_write
            assert next_write > hass.loop.time()
            assert restored.data["rows"] == [[ASSIGNMENT["source"], sample()]]
            assert list(restored.blocked_batches.values()) == [frozen]
            assert await restored.next_batch(DEFAULT_CENTRE) is None
            assert list(updates.pending) == [pending]
            # Retrying reports must not bypass the cache write-frequency bound.
            updates.stage_snapshot({**raw, "revision": 9})
            for _ in range(2):
                resume.set()
                assert await asyncio.wait_for(retries.get(), 5) == 30
                assert cache.await_count == 1
                assert updates.snapshot_next_write == next_write
                assert updates.snapshot_pending["revision"] == 9
                assert list(updates.pending) == [pending]
        finally:
            # Native stop still drains the report reserve and the latest snapshot.
            resume.set()
            await updates.stop()
            assert await hass.config_entries.async_unload(entry.entry_id)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["snapshot"]["revision"] == 9
    assert restored.data["rows"] == [[ASSIGNMENT["source"], sample()], list(pending)]
    assert list(restored.blocked_batches.values()) == [frozen]
    assert not updates.pending
    assert updates.snapshot_timer is None


@pytest.mark.parametrize("version", [1, 3])
async def test_unsupported_store_version_is_rejected_without_rewriting(hass, real_storage, version):
    from pathlib import Path

    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "unsupported-version")
    await Store(hass, version, spool.key, private=True, atomic_writes=True).async_save(
        {"rows": [[ASSIGNMENT["source"], sample()]], "batch": None, "cursors": {}}
    )
    path = Path(spool.store.path)
    before = await hass.async_add_executor_job(path.read_bytes)
    with pytest.raises(HomeAssistantError):
        await spool.initialize()
    assert await hass.async_add_executor_job(path.read_bytes) == before


@pytest.mark.parametrize("lookahead", [False, True])
@pytest.mark.parametrize("millis_offset", [0, 1, 123])
async def test_real_recorder_float_boundary_split_preserves_live_identities(
    recorder_db_url, hass, entry, async_setup_recorder_instance, tmp_path, lookahead, millis_offset
):
    from custom_components.registros_camaras.history import read_page
    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder.tasks import SynchronizeTask

    await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    updates = Updates(hass, entry, AsyncMock())
    updates.assignments = [ASSIGNMENT]
    updates.subscribe()
    boundary = datetime.now(UTC).replace(microsecond=millis_offset * 1000).timestamp()
    start, split, end = (
        int(boundary * 1000) - 1000,
        int(boundary * 1000),
        int(boundary * 1000) + 1000,
    )
    try:
        for at, value in [
            (boundary - 0.1, 10),
            (math.nextafter(boundary, -math.inf), 11),
            (boundary + 0.1, 12),
        ]:
            with patch("homeassistant.core.time.time", return_value=at):
                hass.states.async_set(
                    ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C"}
                )
                await hass.async_block_till_done()
        expected = [{**row, "mode": "history"} for _, row in updates.pending]
    finally:
        updates.unsubscribe()
    recorder = get_instance(hass)
    committed = hass.loop.create_future()
    recorder.queue_task(SynchronizeTask(committed))
    await committed

    async def page(first, last):
        return await recorder.async_add_executor_job(
            read_page, hass, ASSIGNMENT["entityId"], first, last
        )

    combined = await page(start, end)
    with patch(
        "custom_components.registros_camaras.history.PAGE_REPORTS", 1 if lookahead else 1000
    ):
        first = await page(start, end if lookahead else split)
    assert first.end == split
    second = await page(first.end, end)

    def readings(states):
        return [reading(ASSIGNMENT, state, state.last_updated, "history") for state in states]

    assert readings(combined.states) == expected
    assert readings(first.states + second.states) == expected
    assert [row["value"] for row in expected] == [10, 11, 12]


async def test_full_queue_writer_commits_fitting_live_prefix_and_delivers_in_order(
    hass, entry, real_storage
):
    client = AsyncMock()
    client.centre_id = "centre"
    updates = Updates(hass, entry, client)
    await updates.spool.initialize()
    await updates.spool.append([(ASSIGNMENT["source"], sample())])
    blocked = await updates.spool.next_batch("centre")
    await updates.spool.block(blocked["commandId"], "assignment_window")
    healthy = [{**sample(n), "assignmentId": "healthy"} for n in (1, 2)]
    updates.pending.extend((ASSIGNMENT["source"], row) for row in healthy)
    delivered = []
    completed = asyncio.Event()
    original_ack = updates.spool.acknowledge
    original_append = updates.spool.append
    ack_times = []
    persisted_times = []

    async def push(batch):
        delivered.extend(batch["readings"])

    async def acknowledged(command_id):
        await original_ack(command_id)
        ack_times.append(hass.loop.time())
        if len(delivered) == 2:
            completed.set()

    async def persisted(rows, **kwargs):
        await original_append(rows, **kwargs)
        persisted_times.append(hass.loop.time())

    with (
        patch("custom_components.registros_camaras.update_store.MAX_REPORTS", 2),
        patch.object(client, "push_updates", side_effect=push),
        patch.object(updates.spool, "acknowledge", side_effect=acknowledged),
        patch.object(updates.spool, "append", side_effect=persisted),
    ):
        updates.tasks = [
            asyncio.create_task(updates.write_loop()),
            asyncio.create_task(updates.send_loop()),
        ]
        updates.write_ready.set()
        try:
            # Production waits: the 2s batching delay remains, capacity retry is 30s.
            await asyncio.wait_for(completed.wait(), 6)
            assert len(persisted_times) == 2
            assert 0 <= persisted_times[1] - ack_times[0] < 1
            assert not updates.pending
            restored = UpdateStore(hass, entry.entry_id)
            await restored.initialize()
            assert delivered == healthy
            assert restored.data["rows"] == [[ASSIGNMENT["source"], sample()]]
            assert list(restored.blocked_batches.values()) == [blocked]
            assert await restored.next_batch("centre") is None
        finally:
            await updates.stop()


async def test_capacity_signal_does_not_interrupt_real_storage_error_backoff(
    hass, entry, real_storage
):
    from homeassistant.util.file import WriteError

    updates = Updates(hass, entry, AsyncMock())
    await updates.spool.initialize()
    updates.pending.append((ASSIGNMENT["source"], sample()))
    entered = asyncio.Event()
    returned = asyncio.Event()
    original_wait = updates.wait
    original_write = Store._async_write_data

    async def observe_wait(seconds):
        assert seconds == 30
        entered.set()
        await original_wait(seconds)
        returned.set()

    async def fail_segment(store, data):
        if "/segment." in store.key:
            raise WriteError("fixture disk error")
        await original_write(store, data)

    with patch.object(updates, "wait", side_effect=observe_wait):
        try:
            with patch.object(Store, "_async_write_data", fail_segment):
                updates.tasks = [asyncio.create_task(updates.write_loop())]
                updates.write_ready.set()
                await asyncio.wait_for(entered.wait(), 5)
                assert updates.error == "Local persistence failed (HomeAssistantError)"
                updates.capacity_ready.set()
                with pytest.raises(TimeoutError):
                    await asyncio.wait_for(returned.wait(), 0.1)
                assert list(updates.pending) == [(ASSIGNMENT["source"], sample())]
                assert await updates.spool.size() == 0
        finally:
            # Shutdown interrupts the genuine 30s backoff and flushes the report.
            await asyncio.wait_for(updates.stop(), 5)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["rows"] == [[ASSIGNMENT["source"], sample()]]


async def test_initial_cache_write_failure_remains_setup_retry_and_recovers(
    hass, entry, client, real_storage
):
    from pytest_homeassistant_custom_component.common import async_fire_time_changed

    from homeassistant.config_entries import ConfigEntryState
    from homeassistant.util.file import WriteError

    original = Store._async_write_data

    async def fail_cache(store, data):
        if "/cache." in store.key:
            raise WriteError("fixture cache failure")
        await original(store, data)

    with patch.object(Store, "_async_write_data", fail_cache):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state is ConfigEntryState.SETUP_RETRY
    loaded = asyncio.Event()
    unsubscribe = entry.async_on_state_change(
        lambda: loaded.set() if entry.state is ConfigEntryState.LOADED else None
    )
    try:
        async_fire_time_changed(hass, datetime.now(UTC) + timedelta(seconds=10))
        await asyncio.wait_for(loaded.wait(), 5)
    finally:
        unsubscribe()
    assert entry.state is ConfigEntryState.LOADED
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["snapshot"]["revision"] == 7
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_interleaved_assignment_ack_write_volume(hass, real_storage):
    from pathlib import Path

    spool = UpdateStore(hass, "interleaved")
    await spool.initialize()
    rows = [
        [ASSIGNMENT["source"], {**sample(n), "assignmentId": f"assignment-{n % 36}"}]
        for n in range(3600)
    ]
    writes = []
    original = Store._async_write_data

    async def measure(store, data):
        await original(store, data)
        size = await hass.async_add_executor_job(lambda: Path(store.path).stat().st_size)
        writes.append(("/segment." in store.key, size))

    with patch.object(Store, "_async_write_data", measure):
        await spool.append(rows)
        enqueue = sum(size for segment, size in writes if segment)
        enqueue_writes = sum(segment for segment, _ in writes)
        writes.clear()
        spool = UpdateStore(hass, spool.key.removeprefix("registros_camaras.updates."))
        await spool.initialize()
        delivered = []
        while batch := await spool.next_batch("centre"):
            delivered.extend(batch["readings"])
            await spool.acknowledge(batch["commandId"])
        ack_bytes = sum(size for segment, size in writes if segment)
        ack_writes = sum(segment for segment, _ in writes)
        print(
            f"interleaved: enqueue_segment_bytes={enqueue} "
            f"writes={enqueue_writes}; "
            f"drain_segment_bytes={ack_bytes} writes={ack_writes}; "
            f"drain_all_bytes={sum(size for _, size in writes)}"
        )
        assert sorted(delivered, key=lambda row: row["sourceEventId"]) == sorted(
            [row for _, row in rows], key=lambda row: row["sourceEventId"]
        )
        assert await spool.size() == 0
        assert ack_bytes == 0
        assert ack_writes == 0


@pytest.mark.parametrize("closed", [False, True])
@pytest.mark.parametrize("from_epoch", [False, True])
async def test_real_recorder_fresh_manifest_backfills_retained_days_without_empty_windows(
    recorder_db_url,
    hass,
    entry,
    async_setup_recorder_instance,
    tmp_path,
    real_storage,
    closed,
    from_epoch,
):
    from custom_components.registros_camaras.history import read_page

    recorder = await async_setup_recorder_instance(
        hass, {"db_url": f"sqlite:///{tmp_path / 'recorder.db'}", "commit_interval": 60}
    )
    now = datetime.now(UTC).replace(microsecond=0)
    originals = []
    # Real Recorder retains sparse rows well before this integration's first startup.
    # The last row is inside the settlement margin and must remain pending.
    for age, value in [
        (timedelta(days=7), 17),
        (timedelta(days=2), 20),
        (timedelta(hours=3), 23),
        (timedelta(seconds=5), 25),
    ]:
        with patch("homeassistant.core.time.time", return_value=(now - age).timestamp()):
            hass.states.async_set(ASSIGNMENT["entityId"], str(value), {"unit_of_measurement": "°C"})
            await hass.async_block_till_done()
            originals.append(hass.states.get(ASSIGNMENT["entityId"]))
    await recorder.async_block_till_done()
    assignment = {
        **ASSIGNMENT,
        "historyFrom": 0 if from_epoch else int((now - timedelta(days=3)).timestamp() * 1000),
        "endsAt": int((now - timedelta(days=1)).timestamp() * 1000) if closed else None,
    }
    absent = {**assignment, "id": "absent", "entityId": "sensor.never_recorded"}
    hass.config.external_url = ASSIGNMENT["source"]
    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    await updates.apply_manifest(manifest([assignment, absent]))
    assert await updates.spool.cursor(assignment["id"], -1) == assignment["historyFrom"]
    expected_end = assignment["endsAt"] if closed else int(now.timestamp() * 1000) - 10000
    expected = [
        reading(assignment, state, state.last_updated, "history")
        for state in originals
        if assignment["historyFrom"] <= int(state.last_updated.timestamp() * 1000) < expected_end
    ]
    try:
        with (
            patch("custom_components.registros_camaras.history.PAGE_REPORTS", 2),
            patch(
                "custom_components.registros_camaras.updates.read_page", wraps=read_page
            ) as query,
            patch("custom_components.registros_camaras.updates.datetime") as clock,
        ):
            clock.now.return_value = now
            for _ in range(3):
                more = await updates.recover()
                restored = UpdateStore(hass, entry.entry_id)
                await restored.initialize()
                updates.spool = restored
                if not more:
                    break
            assert not more
            assert (
                query.call_count <= 3
            )  # Two actual pages + one empty entity, never epoch windows.
            assert query.call_args_list[0].args[2] == assignment["historyFrom"]
        batch = await updates.spool.next_batch("centre")
        assert batch["readings"] == expected
        assert expected and all(row["mode"] == "history" for row in expected)
        for item in (assignment, absent):
            assert await updates.spool.cursor(item["id"], -1) == expected_end
        await updates.spool.acknowledge(batch["commandId"])
        # Applying a refreshed manifest or restarting never silently rewinds an existing cursor.
        await updates.apply_manifest(manifest([assignment, absent]))
        with patch("custom_components.registros_camaras.updates.datetime") as clock:
            clock.now.return_value = now
            assert not await updates.recover()
        assert await updates.spool.size() == 0
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        assert await restored.next_batch("centre") is None
        assert await restored.cursor(assignment["id"], -1) == expected_end
    finally:
        updates.unsubscribe()


@pytest.mark.parametrize(
    "damage",
    [
        "inline_rows",
        "inline_cache",
        "unknown_metadata",
        "root_segment",
        "mixed_segment",
        "duplicate_segment",
        "duplicate_report",
        "negative_cursor",
        "boolean_cursor",
        "fractional_cursor",
        "offset",
        "empty_segment",
        "invalid_row",
        "changed_batch",
        "mixed_batch",
        "blocked_payload",
        "missing_blocked",
        "unsafe_key",
        "incomplete_capacity",
    ],
)
async def test_current_restore_rejects_unsupported_layout_and_inconsistent_files(
    hass, real_storage, damage
):
    from copy import deepcopy
    from pathlib import Path
    from uuid import uuid4

    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "strict-restore")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample())])
    batch = await spool.next_batch("centre")
    if damage in {"blocked_payload", "missing_blocked"}:
        await spool.block(batch["commandId"], "assignment_window")
    metadata = await spool.store.async_load()
    descriptor = metadata["segments"][0]
    segment = spool.make_store(descriptor["key"])
    rows = await segment.async_load()
    if damage == "inline_rows":
        metadata.pop("segments")
        metadata["rows"] = rows
    elif damage == "inline_cache":
        metadata["manifest"] = manifest()
    elif damage == "unknown_metadata":
        metadata["future_state"] = {"preserve": True}
    elif damage == "root_segment":
        descriptor["key"] = f"{spool.key}.segment.{uuid4()}"
    elif damage == "mixed_segment":
        rows.append([ASSIGNMENT["source"], {**sample(1), "assignmentId": "other"}])
    elif damage == "duplicate_segment":
        metadata["segments"].append(deepcopy(descriptor))
    elif damage == "duplicate_report":
        rows.append(deepcopy(rows[0]))
    elif damage == "negative_cursor":
        metadata["cursors"][ASSIGNMENT["id"]] = -1
    elif damage == "boolean_cursor":
        metadata["cursors"][ASSIGNMENT["id"]] = True
    elif damage == "fractional_cursor":
        metadata["cursors"][ASSIGNMENT["id"]] = 1.5
    elif damage == "offset":
        descriptor["start"] = len(rows)
    elif damage == "empty_segment":
        rows.clear()
    elif damage == "invalid_row":
        rows[0][1]["value"] = None
    elif damage == "changed_batch":
        metadata["batch"]["readings"][0]["value"] = 99
    elif damage == "mixed_batch":
        other = {**sample(1), "assignmentId": "other"}
        other_key = f"{spool.key}.files/segment.{uuid4()}"
        await spool.make_store(other_key).async_save([[ASSIGNMENT["source"], other]])
        metadata["segments"].append({"key": other_key, "start": 0})
        metadata["batch"]["readings"].append(other)
    elif damage == "blocked_payload":
        blocked = spool.make_store(metadata["blocked"][0]["key"])
        frozen = await blocked.async_load()
        frozen["readings"][0]["value"] = 99
        await blocked.async_save(frozen)
    elif damage == "missing_blocked":
        await spool.make_store(metadata["blocked"][0]["key"]).async_remove()
    elif damage == "unsafe_key":
        metadata["cache_key"] = "../another-integration"
    elif damage == "incomplete_capacity":
        rows = [
            [ASSIGNMENT["source"], {**sample(n), "mode": "history", "unit": ""}] for n in range(3)
        ]
        metadata["batch"] = None
    await segment.async_save(rows)
    await spool.store.async_save(metadata)
    # A failure must precede garbage collection and leave every current file intact.
    orphan = spool.make_store(f"{spool.key}.files/segment.{uuid4()}")
    await orphan.async_save([[ASSIGNMENT["source"], sample(9)]])
    index = Path(spool.store.path)
    before = await hass.async_add_executor_job(index.read_bytes)
    with patch("custom_components.registros_camaras.update_store.MAX_INCOMPLETE_REPORTS", 2):
        with pytest.raises(HomeAssistantError, match="Invalid stored"):
            await UpdateStore(hass, "strict-restore").initialize()
    assert await hass.async_add_executor_job(index.read_bytes) == before
    assert await orphan.async_load() is not None


@pytest.mark.parametrize("content", ["batch", "cursor", "cache"])
async def test_nonempty_unbound_destination_is_rejected(hass, real_storage, content):
    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "unbound")
    await spool.initialize()
    frozen = None
    if content == "batch":
        await spool.append([(ASSIGNMENT["source"], sample())])
        frozen = await spool.next_batch("centre")
    elif content == "cursor":
        await spool.ensure_cursor(ASSIGNMENT["id"], 100)
    else:
        await spool.cache(manifest=manifest())
    with pytest.raises(HomeAssistantError, match="no destination"):
        await spool.bind("a" * 64)
    restored = UpdateStore(hass, "unbound")
    await restored.initialize()
    assert await restored.next_batch("centre") == frozen
    assert "destination" not in restored.data


async def test_invalid_rebuildable_manifest_cache_cannot_replace_queue(hass, real_storage):
    spool = UpdateStore(hass, "bad-cache")
    await spool.initialize()
    await spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await spool.next_batch("centre")
    await spool.cache(manifest={"assignments": [ASSIGNMENT]})
    restored = UpdateStore(hass, "bad-cache")
    await restored.initialize()
    assert restored.cache_key is None
    assert "manifest" not in restored.data
    assert await restored.next_batch("centre") == frozen


@pytest.mark.parametrize("damage", ["missing_locations", "wrong_centre", "not_object"])
async def test_invalid_snapshot_cache_keeps_manifest_and_frozen_batch_through_offline_retry(
    hass, entry, client, stream_manifest, real_storage, damage
):
    from copy import deepcopy

    from homeassistant.config_entries import ConfigEntryState

    hass.config.external_url = ASSIGNMENT["source"]
    stream_manifest.return_value["assignments"] = [ASSIGNMENT]
    assert await hass.config_entries.async_setup(entry.entry_id)
    updates = entry.runtime_data.updates
    await updates.spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await updates.spool.next_batch(entry.data["centre_id"])
    cursors = deepcopy(updates.spool.data["cursors"])
    valid_manifest = deepcopy(updates.spool.data["manifest"])
    valid_snapshot = deepcopy(updates.spool.data["snapshot"])
    assert await hass.config_entries.async_unload(entry.entry_id)
    cache = updates.spool.make_store(updates.spool.cache_key)
    corrupted = await cache.async_load()
    if damage == "missing_locations":
        del corrupted["snapshot"]["locations"]
    elif damage == "wrong_centre":
        corrupted["snapshot"]["centreId"] = "another-centre"
        for location in corrupted["snapshot"]["locations"]:
            location["centreId"] = "another-centre"
    else:
        corrupted["snapshot"] = []
    await cache.async_save(corrupted)
    client.side_effect = CannotConnect()
    stream_manifest.side_effect = CannotConnect()
    # Repeat failure to prove the valid manifest survives cleanup and another restore.
    for _ in range(2):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state is ConfigEntryState.SETUP_RETRY
        restored = UpdateStore(hass, entry.entry_id)
        await restored.initialize()
        assert "snapshot" not in restored.data
        assert restored.data["manifest"] == valid_manifest
        assert restored.data["cursors"] == cursors
        assert await restored.next_batch(entry.data["centre_id"]) == frozen
        assert await hass.config_entries.async_unload(entry.entry_id)
    client.side_effect = None
    stream_manifest.side_effect = None
    assert await hass.config_entries.async_setup(entry.entry_id)
    recovered = entry.runtime_data.updates.spool
    assert recovered.data["snapshot"] == valid_snapshot
    assert recovered.data["manifest"] == valid_manifest
    assert recovered.data["cursors"] == cursors
    assert await recovered.next_batch(entry.data["centre_id"]) == frozen
    assert await hass.config_entries.async_unload(entry.entry_id)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert restored.data["snapshot"] == valid_snapshot
    assert await restored.next_batch(entry.data["centre_id"]) == frozen


@pytest.mark.parametrize("failure", ["segment", "index"])
async def test_missing_unit_live_promotion_write_failure_preserves_retry(
    hass, real_storage, failure
):
    from homeassistant.exceptions import HomeAssistantError
    from homeassistant.util.file import WriteError

    spool = UpdateStore(hass, "promotion-failure")
    await spool.initialize()
    live = {**sample(), "unit": ""}
    history = {**live, "mode": "history"}
    await spool.append([(ASSIGNMENT["source"], history)], (ASSIGNMENT["id"], 100))
    original = Store._async_write_data

    async def fail(store, data):
        if (failure == "segment" and "/segment." in store.key) or (
            failure == "index" and store.key == spool.key
        ):
            raise WriteError("fixture promotion failure")
        await original(store, data)

    with patch.object(Store, "_async_write_data", fail):
        with pytest.raises(HomeAssistantError, match="not persisted"):
            await spool.append([(ASSIGNMENT["source"], live)])
    restored = UpdateStore(hass, "promotion-failure")
    await restored.initialize()
    assert restored.data["rows"] == [[ASSIGNMENT["source"], history]]
    assert await restored.cursor(ASSIGNMENT["id"], 0) == 100
    assert await restored.next_batch("centre") is None
    await restored.append([(ASSIGNMENT["source"], live)])
    frozen = await restored.next_batch("centre")
    assert frozen["readings"] == [live]
    reopened = UpdateStore(hass, "promotion-failure")
    await reopened.initialize()
    assert await reopened.next_batch("centre") == frozen
    await reopened.acknowledge(frozen["commandId"])
    assert await reopened.size() == 0


async def test_missing_unit_overlap_cannot_rewrite_frozen_history(hass, real_storage):
    from homeassistant.exceptions import HomeAssistantError

    spool = UpdateStore(hass, "frozen-history")
    await spool.initialize()
    history = {**sample(), "mode": "history", "unit": "", "status": "unavailable", "value": None}
    await spool.append([(ASSIGNMENT["source"], history)])
    frozen = await spool.next_batch("centre")
    # Non-numeric history is already eligible; a later unit enrichment cannot
    # alter the payload owned by its current frozen command.
    with pytest.raises(HomeAssistantError, match="frozen"):
        await spool.append([(ASSIGNMENT["source"], {**history, "mode": "live", "unit": "°C"})])
    restored = UpdateStore(hass, "frozen-history")
    await restored.initialize()
    assert await restored.next_batch("centre") == frozen
    assert restored.data["rows"] == [[ASSIGNMENT["source"], history]]


async def test_snapshot_cache_requires_matching_store_destination(hass, entry, raw, real_storage):
    from homeassistant.exceptions import HomeAssistantError

    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    spool = updates.spool
    destination = spool.data["destination"]
    await spool.append([(ASSIGNMENT["source"], sample())])
    frozen = await spool.next_batch(entry.data["centre_id"])
    await spool.cache(snapshot=raw, manifest=manifest())
    metadata = await spool.store.async_load()
    metadata["destination"] = "a" * 64
    await spool.store.async_save(metadata)
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert "snapshot" not in restored.data
    assert restored.data["manifest"] == manifest()
    assert await restored.next_batch(entry.data["centre_id"]) == frozen
    with pytest.raises(HomeAssistantError, match="another destination"):
        await restored.bind(destination)


async def test_invalid_manifest_cache_retains_valid_bound_snapshot(hass, entry, raw, real_storage):
    updates = Updates(hass, entry, AsyncMock())
    await updates.prepare()
    await updates.spool.cache(snapshot=raw, manifest={"assignments": [ASSIGNMENT]})
    restored = UpdateStore(hass, entry.entry_id)
    await restored.initialize()
    assert "manifest" not in restored.data
    assert restored.data["snapshot"] == raw
    # A later verified write replaces only the invalid cache portion.
    await restored.cache(manifest=manifest())
    reopened = UpdateStore(hass, entry.entry_id)
    await reopened.initialize()
    assert reopened.data["snapshot"] == raw
    assert reopened.data["manifest"] == manifest()
