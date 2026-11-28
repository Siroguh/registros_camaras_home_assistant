"""All selected HA reports -> durable outbox -> versioned Alchemy ingestion API."""

import asyncio
from collections import deque
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
import logging
import math
import random
from typing import TYPE_CHECKING
from uuid import uuid4

from homeassistant.const import CONF_URL, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HassJob, HomeAssistant, callback
from homeassistant.helpers import entity_registry
from homeassistant.helpers.event import (
    async_track_state_change_event,
    async_track_state_report_event,
)

from .api import CannotConnect, IncompatibleApi, RegistrosClient, UpdatesRejected, normalize_url
from .const import CONF_CENTRE_ID, DOMAIN
from .history import HistoryWindowFull, read_page
from .update_store import (
    MAX_REPORTS,
    MEMORY_REPORTS,
    IncompleteHistoryFull,
    ReportCapacityFull,
    UpdateStore,
)

if TYPE_CHECKING:
    from .coordinator import RegistrosConfigEntry

_LOGGER = logging.getLogger(__name__)
SNAPSHOT_INTERVAL_SECONDS = 60


def reading(assignment: dict, state, at: datetime, mode: str) -> dict:
    """Copy immediately: HA mutates State.last_reported on equal-value reports."""
    try:
        value = float(state.state)
    except ValueError, TypeError:
        value = math.nan
    return {
        "assignmentId": assignment["id"],
        "sourceEventId": at.astimezone(UTC).isoformat()
        + ":change:"
        + hashlib.sha256(state.state.encode()).hexdigest(),
        "observedAt": int(at.timestamp() * 1000),
        "value": value if math.isfinite(value) else None,
        "unit": str(state.attributes.get("unit_of_measurement", ""))[:20],
        "status": (
            "valid"
            if math.isfinite(value)
            else "unavailable"
            if state.state == "unavailable"
            else "invalid"
        ),
        "mode": mode,
    }


class Updates:
    def __init__(
        self, hass: HomeAssistant, entry: RegistrosConfigEntry, client: RegistrosClient
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.client = client
        self.spool = UpdateStore(hass, entry.entry_id)
        self.pending: deque[tuple[str, dict]] = deque()
        self.assignments: list[dict] = []
        self.unsubscribers: list[Callable[[], None]] = []
        self.tasks: list[asyncio.Task] = []
        self.write_ready = asyncio.Event()
        self.capacity_ready = asyncio.Event()
        self.send_ready = asyncio.Event()
        self.stop_ready = asyncio.Event()
        self.stopping = False
        self.paused = False
        self.error: str | None = None
        self.recovery_error: str | None = None
        self.unrecoverable_reports = 0
        self.prepared = False
        self.change_counts: dict[str, tuple[datetime, dict[str, int]]] = {}
        self.stop_task: asyncio.Task | None = None
        self.snapshot_pending: dict | None = None
        self.snapshot_next_write = 0.0
        self.snapshot_timer: asyncio.TimerHandle | None = None
        self.cache_error: str | None = None
        self.recovery_offset = 0

    async def prepare(self) -> None:
        if self.prepared:
            return
        await self.spool.initialize()
        destination = hashlib.sha256(
            json.dumps(
                [normalize_url(self.entry.data[CONF_URL]), self.entry.data[CONF_CENTRE_ID]]
            ).encode()
        ).hexdigest()
        await self.spool.bind(destination)
        self.prepared = True
        self.paused = await self.spool.deliverable_size() >= MAX_REPORTS
        if "manifest" in self.spool.data:
            # Install listeners before either HTTP request can wait on an offline server.
            await self.apply_manifest(self.spool.data["manifest"])

    async def start(self) -> None:
        await self.prepare()
        self.paused = await self.spool.deliverable_size() >= MAX_REPORTS
        try:
            await self.refresh()
        except CannotConnect:
            if "manifest" not in self.spool.data:
                raise
            await self.apply_manifest(self.spool.data["manifest"])
            self.recovery_error = "Offline: capturing with last verified sensor mappings"
        for name, coroutine in (
            ("writer", self.write_loop()),
            ("sender", self.send_loop()),
            ("reconciliation", self.reconcile_loop()),
        ):
            self.tasks.append(
                self.entry.async_create_background_task(
                    self.hass,
                    coroutine,
                    f"registros updates {name}",
                    eager_start=False,
                )
            )
        self.entry.async_on_unload(self.hass.async_add_shutdown_job(HassJob(self.stop)))
        self.entry.async_on_unload(
            self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self.on_stop)
        )
        self.send_ready.set()

    def unsubscribe(self) -> None:
        for unsubscribe in self.unsubscribers:
            unsubscribe()
        self.unsubscribers.clear()

    async def stop(self) -> None:
        self.stopping = True
        if self.snapshot_timer is not None:
            self.snapshot_timer.cancel()
            self.snapshot_timer = None
        self.unsubscribe()
        self.stop_ready.set()
        self.capacity_ready.set()
        self.write_ready.set()
        self.send_ready.set()
        if self.stop_task is None or (
            self.stop_task.done()
            and (self.stop_task.cancelled() or self.stop_task.exception() is not None)
        ):
            self.stop_task = self.hass.async_create_task(
                self.finish_stop(), "registros flush on stop"
            )
        # HA may time out the shutdown job; the local flush must outlive that caller.
        await asyncio.shield(self.stop_task)

    async def on_stop(self, _: Event) -> None:
        await self.stop()

    async def finish_stop(self) -> None:
        # Network work cannot hold up RAM persistence. A cancelled sender retains its batch.
        for task in self.tasks[1:]:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks[:1], return_exceptions=True)
        if self.pending:
            await self.spool.append(list(self.pending), shutdown=True)
            self.pending.clear()
        await self.flush_snapshot()
        await asyncio.gather(*self.tasks[1:], return_exceptions=True)

    @callback
    def stage_snapshot(self, snapshot: dict) -> None:
        """Coalesce only the rebuildable dashboard cache, never sensor reports."""
        self.snapshot_pending = deepcopy(snapshot)
        self.write_ready.set()

    async def flush_snapshot(self) -> None:
        snapshot = self.snapshot_pending
        if snapshot is not None:
            await self.spool.cache(snapshot=snapshot)
            if self.snapshot_pending is snapshot:
                self.snapshot_pending = None
            self.snapshot_next_write = self.hass.loop.time() + SNAPSHOT_INTERVAL_SECONDS
            self.cache_error = None

    def schedule_snapshot(self, delay: float) -> None:
        if self.snapshot_timer is not None:
            self.snapshot_timer.cancel()
        self.snapshot_timer = self.hass.loop.call_later(delay, self.write_ready.set)

    async def wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_ready.wait(), seconds)
        except TimeoutError:
            pass

    async def wait_for_capacity(self) -> None:
        """A durable ACK or shutdown interrupts capacity retry, not storage backoff."""
        try:
            await asyncio.wait_for(self.capacity_ready.wait(), 30)
        except TimeoutError:
            pass

    async def refresh(self) -> None:
        manifest = await self.client.sensor_stream()
        await self.apply_manifest(manifest)
        await self.spool.cache(manifest=manifest)

    async def apply_manifest(self, manifest: dict) -> None:
        sources = {
            url.rstrip("/")
            for url in (self.hass.config.external_url, self.hass.config.internal_url)
            if url
        }
        assignments = [a for a in manifest["assignments"] if a["source"].rstrip("/") in sources]
        if manifest["assignments"] and not assignments:
            self.recovery_error = (
                "HA internal/external URL does not match a configured sensor source"
            )
        registry = entity_registry.async_get(self.hass)
        if any(
            (registered := registry.async_get(a["entityId"])) is not None
            and registered.platform == DOMAIN
            for a in assignments
        ):
            raise IncompatibleApi("Registros output entities cannot be sensor inputs")
        for assignment in assignments:
            await self.spool.ensure_cursor(
                assignment["id"],
                assignment["historyFrom"],
            )
        self.assignments = assignments
        self.subscribe()

    def subscribe(self) -> None:
        self.unsubscribe()
        if self.paused or self.stopping:
            return
        entities = {a["entityId"] for a in self.assignments if a["endsAt"] is None}
        if entities:
            self.unsubscribers = [
                async_track_state_change_event(self.hass, entities, self.capture),
                async_track_state_report_event(self.hass, entities, self.capture),
            ]

    @callback
    def capture(self, event: Event) -> None:
        state = event.data.get("new_state")
        if state is None:
            return
        at = event.data.get("last_reported", state.last_updated)
        for assignment in self.assignments:
            if assignment["entityId"] != state.entity_id or assignment["endsAt"] is not None:
                continue
            row = reading(assignment, state, at, "live")
            if "last_reported" in event.data:
                # Equal reports are not Recorder rows; even coincident timestamps are distinct.
                row["sourceEventId"] += f":report:{uuid4()}"
            else:
                previous_at, counts = self.change_counts.get(assignment["id"], (at, {}))
                if previous_at != at:
                    counts = {}
                key = row["sourceEventId"]
                occurrence = counts.get(key, 0)
                counts[key] = occurrence + 1
                self.change_counts[assignment["id"]] = (at, counts)
                if occurrence:
                    row["sourceEventId"] += f":occurrence:{occurrence}"
            if row["observedAt"] < assignment["startsAt"]:
                continue
            if len(self.pending) >= MEMORY_REPORTS:
                self.paused = True
                self.error = "Local buffer full: capture paused; equal-value reports may be missing"
                self.unrecoverable_reports += 1
                self.unsubscribe()
                _LOGGER.error(self.error)
                break
            self.pending.append((assignment["source"], row))
        self.write_ready.set()

    async def write_loop(self) -> None:
        while not self.stopping:
            await self.write_ready.wait()
            self.write_ready.clear()
            while not self.stopping:
                # Service the cache between report attempts, even if capacity stays full.
                # A due snapshot staged during the 30s retry wait gets a turn on retry.
                if self.snapshot_pending is not None:
                    delay = self.snapshot_next_write - self.hass.loop.time()
                    if delay > 0:
                        self.schedule_snapshot(delay)
                    else:
                        try:
                            await self.flush_snapshot()
                        except Exception as err:
                            self.cache_error = f"Dashboard cache pending ({type(err).__name__})"
                            self.snapshot_next_write = self.hass.loop.time() + 30
                            self.schedule_snapshot(30)
                if not self.pending or self.stopping:
                    break
                # Clear before checking capacity so a concurrent ACK cannot be lost.
                self.capacity_ready.clear()
                rows = list(self.pending)[:1000]
                try:
                    while True:
                        try:
                            await self.spool.append(rows)
                            break
                        except ReportCapacityFull:
                            # Blocked rows may leave less space than this chunk forever.
                            # Only a successfully committed prefix can leave RAM.
                            if len(rows) == 1:
                                raise
                            rows = rows[: len(rows) // 2]
                except ReportCapacityFull:
                    self.error = "Local persistence pending (ReportCapacityFull)"
                    await self.wait_for_capacity()
                    continue
                except Exception as err:
                    # Never remove a report when persistence fails.
                    self.error = f"Local persistence failed ({type(err).__name__})"
                    _LOGGER.error(self.error)
                    await self.wait(30)
                    continue
                for _ in rows:
                    self.pending.popleft()
                self.send_ready.set()
                if self.paused and await self.spool.deliverable_size() < MAX_REPORTS:
                    self.paused = False
                    self.subscribe()

    async def send_loop(self) -> None:
        retry = 1.0
        while not self.stopping:
            await self.send_ready.wait()
            self.send_ready.clear()
            await self.wait(2)
            while not self.stopping:
                try:
                    batch = await self.spool.next_batch(self.client.centre_id)
                    if await self.spool.deliverable_size() >= MAX_REPORTS:
                        self.paused = True
                        self.unsubscribe()
                        self.recovery_error = (
                            "HA storage queue full: capture paused; reports may be missing"
                        )
                    elif self.paused and not self.pending:
                        self.paused = False
                        self.subscribe()
                    if batch is None:
                        break
                    try:
                        await self.client.push_updates(batch)
                    except UpdatesRejected as err:
                        await self.spool.block(err.command_id, err.reason)
                    else:
                        await self.spool.acknowledge(batch["commandId"])
                        self.capacity_ready.set()
                    retry = 1.0
                    self.error = None
                except Exception as err:
                    self.error = f"Update delivery pending ({type(err).__name__})"
                    await self.wait(retry + random.uniform(0, retry / 4))
                    retry = min(300, retry * 2)

    async def recover(self) -> bool:
        """Recorder supplements the outbox; it cannot reconstruct identical reports."""
        if "recorder" not in self.hass.config.components:
            self.recovery_error = (
                "Recorder unavailable; only persisted live reports can be recovered"
            )
            return False
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.tasks import SynchronizeTask

        until = int(datetime.now(UTC).timestamp() * 1000) - 10000
        recorder = get_instance(self.hass)
        # Freeze the window first, then synchronize the writer before querying readers.
        # Always enqueue: Recorder's optional fast-path future can miss a dequeued event.
        committed = self.hass.loop.create_future()
        recorder.queue_task(SynchronizeTask(committed))
        async with asyncio.timeout(30):
            await committed
        more = False
        self.recovery_error = None
        if await self.spool.deliverable_size() >= MAX_REPORTS:
            return False
        assignments = (
            self.assignments[self.recovery_offset :] + self.assignments[: self.recovery_offset]
        )
        self.recovery_offset = (self.recovery_offset + 1) % max(1, len(self.assignments))
        for assignment in assignments:
            if self.stopping:
                return False
            target = min(until, assignment["endsAt"]) if assignment["endsAt"] is not None else until
            start = await self.spool.cursor(assignment["id"], assignment["historyFrom"])
            # The indexed native reader caps actual rows, not elapsed time. An empty
            # interval (including epoch -> retained history) costs one bounded query.
            end = target
            if end <= start:
                continue
            try:
                page = await recorder.async_add_executor_job(
                    read_page, self.hass, assignment["entityId"], start, end
                )
            except HistoryWindowFull:
                self.recovery_error = (
                    "Coincident Recorder group exceeds page capacity; window pending"
                )
                continue
            end = page.end
            rows = []
            counts: dict[str, int] = {}
            for state in page.states:
                if not start <= int(state.last_updated.timestamp() * 1000) < end:
                    continue
                row = reading(assignment, state, state.last_updated, "history")
                key = row["sourceEventId"]
                occurrence = counts.get(key, 0)
                counts[key] = occurrence + 1
                if occurrence:
                    row["sourceEventId"] += f":occurrence:{occurrence}"
                rows.append((assignment["source"], row))
            try:
                while True:
                    try:
                        await self.spool.append(rows, (assignment["id"], end))
                        break
                    except ReportCapacityFull:
                        # Trim at a whole millisecond, never re-number occurrences
                        # within a timestamp or advance past an unpersisted row.
                        boundary = rows[len(rows) // 2][1]["observedAt"] if rows else start
                        if boundary <= start:
                            raise
                        end = boundary
                        rows = [item for item in rows if item[1]["observedAt"] < end]
            except IncompleteHistoryFull:
                self.recovery_error = "Incomplete sensor history capacity reached; window pending"
                continue
            except ReportCapacityFull:
                self.recovery_error = "Sensor history capacity reached; window pending"
                continue
            more = more or end < target
            if rows:
                self.send_ready.set()
        return more

    async def reconcile_loop(self) -> None:
        next_refresh = self.hass.loop.time() + 300
        while not self.stopping:
            more = False
            try:
                more = await self.recover()
            except Exception as err:
                self.recovery_error = f"Recorder recovery pending ({type(err).__name__})"
            await self.wait(0.1 if more else 300)
            if not self.stopping and self.hass.loop.time() >= next_refresh:
                try:
                    await self.refresh()
                except Exception as err:
                    self.recovery_error = f"Sensor mapping refresh pending ({type(err).__name__})"
                next_refresh = self.hass.loop.time() + 300

    def diagnostics(self) -> dict:
        return {
            "queued": len(self.spool.data["rows"]),
            "queue_capacity": MAX_REPORTS,
            "awaiting_local_persistence": len(self.pending),
            "capture_paused": self.paused,
            "delivery_error": self.error,
            "recovery_error": self.recovery_error,
            "capture_overflows": self.unrecoverable_reports,
            "dashboard_cache_pending": self.snapshot_pending is not None,
            "dashboard_cache_error": self.cache_error,
            **self.spool.blocked_diagnostics(),
        }
