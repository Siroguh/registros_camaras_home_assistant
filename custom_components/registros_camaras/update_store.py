"""Versioned HA-native storage for pending sensor reports and delivery receipts."""

import asyncio
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
from uuid import UUID, uuid4

from homeassistant.const import CONF_URL, EVENT_HOMEASSISTANT_FINAL_WRITE
from homeassistant.core import CoreState, HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.storage import Store

from .api import IncompatibleApi, decode_sensor_stream, decode_snapshot, normalize_url
from .const import CONF_CENTRE_ID

MAX_REPORTS = 50000
MEMORY_REPORTS = 10000
SEGMENT_REPORTS = 1000
MAX_INCOMPLETE_REPORTS = 1000


class ReportCapacityFull(HomeAssistantError):
    """The bounded delivered-or-blocked report budget is full."""


def incomplete_history(row: dict) -> bool:
    return row["mode"] == "history" and row["status"] == "valid" and not row["unit"]


class IncompleteHistoryFull(HomeAssistantError):
    """The incomplete-history reserve cannot accept this recovery window."""


def identity(source: str, row: dict) -> tuple[str, str, str]:
    return source, row["assignmentId"], row["sourceEventId"]


def require(condition: bool) -> None:
    if not condition:
        raise ValueError("Invalid sensor queue structure")


def is_uuid(value) -> bool:
    return isinstance(value, str) and str(UUID(value)) == value


def validate_row(row) -> None:
    require(
        isinstance(row, dict)
        and row.keys()
        == {"assignmentId", "sourceEventId", "observedAt", "mode", "value", "unit", "status"}
    )
    for name in ("assignmentId", "sourceEventId"):
        require(isinstance(row[name], str) and bool(row[name]))
    require(type(row["observedAt"]) is int and row["observedAt"] >= 0)
    require(row["mode"] in {"live", "history"})
    require(isinstance(row["unit"], str) and len(row["unit"]) <= 20)
    require(row["status"] in {"valid", "invalid", "unavailable"})
    if row["status"] == "valid":
        require(type(row["value"]) in (float, int) and math.isfinite(row["value"]))
    else:
        require(row["value"] is None)


class UpdateStore:
    """Use Store's atomic files, versioning, executor and final-write lifecycle."""

    def __init__(self, hass: HomeAssistant, entry_id: str) -> None:
        self.hass = hass
        self.entry_id = entry_id
        self.key = f"registros_camaras.updates.{entry_id}"
        self.store = self.make_store()
        self.lock = asyncio.Lock()
        self.data = {"rows": [], "batch": None, "cursors": {}, "commit": None}
        self.segments: list[tuple[dict, list]] = []
        self.final_pending = False
        self.garbage: set[str] = set()
        self.cache_key: str | None = None
        self.cached: dict = {}
        self.blocked_batches: dict[str, dict] = {}

    def make_store(self, key: str | None = None) -> Store:
        if key is None or key == self.key:
            return Store(
                self.hass,
                2,
                self.key,
                private=True,
                atomic_writes=True,
                serialize_in_event_loop=False,
            )
        return Store(
            self.hass,
            1,
            key,
            private=True,
            atomic_writes=True,
            serialize_in_event_loop=False,
        )

    async def initialize(self) -> None:
        try:
            value = await self.store.async_load()
        except NotImplementedError as err:
            raise HomeAssistantError("Unsupported sensor queue version") from err
        self.garbage = await self.hass.async_add_executor_job(self.stored_keys)
        if value is None and self.garbage:
            raise HomeAssistantError("Sensor queue index missing; stored files require recovery")
        if value is not None:
            await self.restore(value)
            self.cache_key = value.get("cache_key")
            if self.cache_key:
                cached = await self.make_store(self.cache_key).async_load()
                if valid := self.validate_cache(cached):
                    # Keep the original file until the next verified commit rewrites
                    # any rejected part. A bad snapshot must not delete a valid manifest.
                    self.cached = cached
                    self.data.update(valid)
                else:
                    # The queue is authoritative; this optional cache can be fetched online.
                    self.cache_key = None
        await self.cleanup()

    def validate_cache(self, cached) -> dict:
        """Decode independent rebuildable projections, bound to the configured destination."""
        if not isinstance(cached, dict) or cached.keys() - {"manifest", "snapshot"}:
            return {}
        valid = {}
        if "manifest" in cached:
            try:
                valid["manifest"] = decode_sensor_stream(cached["manifest"])
            except IncompatibleApi:
                pass
        if "snapshot" in cached and (
            entry := self.hass.config_entries.async_get_entry(self.entry_id)
        ):
            try:
                centre = entry.data[CONF_CENTRE_ID]
                require(isinstance(centre, str) and bool(centre))
                destination = hashlib.sha256(
                    json.dumps([normalize_url(entry.data[CONF_URL]), centre]).encode()
                ).hexdigest()
                require(destination == self.data.get("destination"))
                valid["snapshot"] = decode_snapshot(cached["snapshot"], centre).raw
            except IncompatibleApi, KeyError, TypeError, ValueError:
                pass
        return valid

    def validate_key(self, key, kind: str) -> None:
        prefix = f"{self.key}.files/{kind}."
        require(isinstance(key, str) and key.startswith(prefix))
        require(is_uuid(key.removeprefix(prefix)))

    async def restore(self, value) -> None:
        """Accept only the current index and its complete, mutually consistent files.

        Validate everything before installing metadata or collecting orphan files.
        Frozen commands must own exact queued payloads, never reconstructed batches.
        """
        try:
            require(isinstance(value, dict))
            require({"segments", "batch", "cursors", "commit"} <= value.keys())
            require(
                value.keys()
                <= {"segments", "batch", "cursors", "commit", "destination", "blocked", "cache_key"}
            )
            require(is_uuid(value["commit"]))
            require(isinstance(value["cursors"], dict))
            for assignment, cursor in value["cursors"].items():
                require(isinstance(assignment, str) and bool(assignment))
                require(type(cursor) is int and cursor >= 0)
            if "destination" in value:
                destination = value["destination"]
                require(isinstance(destination, str) and len(destination) == 64)
                require(all(char in "0123456789abcdef" for char in destination))
            if "cache_key" in value:
                self.validate_key(value["cache_key"], "cache")
            require(isinstance(value["segments"], list))
            segments = []
            rows = []
            keys = set()
            for descriptor in value["segments"]:
                require(isinstance(descriptor, dict) and descriptor.keys() == {"key", "start"})
                key = descriptor["key"]
                self.validate_key(key, "segment")
                require(key not in keys)
                keys.add(key)
                items = await self.make_store(key).async_load()
                require(isinstance(items, list) and 0 < len(items) <= SEGMENT_REPORTS)
                require(type(descriptor["start"]) is int and 0 <= descriptor["start"] < len(items))
                groups = set()
                for item in items:
                    require(isinstance(item, list) and len(item) == 2)
                    source, row = item
                    require(isinstance(source, str) and bool(source))
                    validate_row(row)
                    groups.add((source, row["assignmentId"]))
                require(len(groups) == 1)
                segments.append((descriptor, items))
                rows.extend(items[descriptor["start"] :])
            queued = {identity(*item): item[1] for item in rows}
            require(len(queued) == len(rows))
            incomplete = sum(incomplete_history(row) for _, row in rows)
            require(incomplete <= MAX_INCOMPLETE_REPORTS)
            require(len(rows) - incomplete <= MAX_REPORTS + MEMORY_REPORTS)
            claimed = set()
            commands = set()

            def validate_batch(batch):
                require(
                    isinstance(batch, dict)
                    and batch.keys()
                    == {"apiVersion", "commandId", "centreId", "source", "readings"}
                )
                require(type(batch["apiVersion"]) is int and batch["apiVersion"] == 1)
                require(is_uuid(batch["commandId"]) and batch["commandId"] not in commands)
                commands.add(batch["commandId"])
                require(isinstance(batch["centreId"], str) and bool(batch["centreId"]))
                require(isinstance(batch["source"], str) and bool(batch["source"]))
                require(isinstance(batch["readings"], list) and 0 < len(batch["readings"]) <= 100)
                assignments = set()
                for row in batch["readings"]:
                    validate_row(row)
                    require(not incomplete_history(row))
                    key = identity(batch["source"], row)
                    require(key not in claimed and queued.get(key) == row)
                    claimed.add(key)
                    assignments.add(row["assignmentId"])
                require(len(assignments) == 1)

            if value["batch"] is not None:
                validate_batch(value["batch"])
            blocked_batches = {}
            require(isinstance(value.get("blocked", []), list))
            for descriptor in value.get("blocked", []):
                require(
                    isinstance(descriptor, dict)
                    and descriptor.keys() == {"key", "commandId", "reason"}
                )
                key = descriptor["key"]
                self.validate_key(key, "blocked")
                require(descriptor["reason"] == "assignment_window")
                require(key == f"{self.key}.files/blocked.{descriptor['commandId']}")
                frozen = await self.make_store(key).async_load()
                validate_batch(frozen)
                require(frozen["commandId"] == descriptor["commandId"])
                blocked_batches[key] = frozen
        except (KeyError, TypeError, ValueError, OverflowError, NotImplementedError) as err:
            raise HomeAssistantError("Invalid stored sensor update queue") from err
        self.segments = segments
        self.data = {**value, "rows": rows}
        self.blocked_batches = blocked_batches

    def stored_keys(self) -> set[str]:
        root = Path(self.store.path).parent
        return {
            f"{self.key}.files/{path.name}"
            for path in (root / f"{self.key}.files").glob("*")
            if path.name.startswith(("segment.", "cache.", "blocked."))
        }

    async def cleanup(self) -> None:
        self.garbage.difference_update(d["key"] for d, _ in self.segments)
        self.garbage.discard(self.cache_key)
        self.garbage.difference_update(b["key"] for b in self.data.get("blocked", []))
        for key in list(self.garbage):
            try:
                await self.make_store(key).async_remove()
            except OSError:
                continue
            self.garbage.remove(key)

    async def save(self, data: dict) -> None:
        if self.hass.state is CoreState.stopping:
            # FINAL_WRITE must commit segments before their metadata, not concurrently.
            self.data = data
            if not self.final_pending:
                self.final_pending = True
                self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_FINAL_WRITE, self.final_write)
            return
        task = self.hass.async_create_task(self.commit(data), "registros durable write")
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Keep the lock even if shutdown cancels a previously cancelled writer.
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def final_write(self, _) -> None:
        async with self.lock:
            await self.save(self.data)
            self.final_pending = False

    async def verified_save(self, store: Store, data) -> None:
        await store.async_save(data)
        if await self.make_store(store.key).async_load() != data:
            raise HomeAssistantError("Sensor update queue was not persisted")

    async def commit(self, data: dict) -> None:
        """Immutable bounded segments, then one atomic metadata commit (rows + cursor + ACK)."""
        remaining = {identity(*item): item for item in data["rows"]}
        known = set()
        segments = []
        for descriptor, items in self.segments:
            start = descriptor["start"]
            while start < len(items) and identity(*items[start]) not in remaining:
                start += 1
            if start < len(items):
                active = [item for item in items[start:] if identity(*item) in remaining]
                if len(active) != len(items) - start or any(
                    item != remaining[identity(*item)] for item in active
                ):
                    items = [remaining[identity(*item)] for item in active]
                    descriptor = {"key": f"{self.key}.files/segment.{uuid4()}", "start": 0}
                    self.garbage.add(descriptor["key"])
                    await self.verified_save(self.make_store(descriptor["key"]), items)
                    start = 0
                segments.append(({**descriptor, "start": start}, items))
                known.update(identity(*item) for item in items[start:])
        added: dict[tuple[str, str], list] = {}
        for item in data["rows"]:
            if identity(*item) not in known:
                added.setdefault((item[0], item[1]["assignmentId"]), []).append(item)
        for group, items in added.items():
            # Match delivery's source + assignment boundary so ACKs trim prefixes.
            tail = next(
                (
                    i
                    for i in range(len(segments) - 1, -1, -1)
                    if (segments[i][1][0][0], segments[i][1][0][1]["assignmentId"]) == group
                ),
                None,
            )
            if tail is not None:
                descriptor, prior = segments[tail]
                active = prior[descriptor["start"] :]
                if len(active) < SEGMENT_REPORTS:
                    segments.pop(tail)
                    items = [*active, *items]
            for offset in range(0, len(items), SEGMENT_REPORTS):
                chunk = items[offset : offset + SEGMENT_REPORTS]
                descriptor = {"key": f"{self.key}.files/segment.{uuid4()}", "start": 0}
                self.garbage.add(descriptor["key"])
                await self.verified_save(self.make_store(descriptor["key"]), chunk)
                segments.append((descriptor, chunk))
        cached = {k: data[k] for k in ("snapshot", "manifest") if k in data}
        cache_key = self.cache_key
        if cached != self.cached:
            cache_key = f"{self.key}.files/cache.{uuid4()}" if cached else None
            if cache_key:
                self.garbage.add(cache_key)
                await self.verified_save(self.make_store(cache_key), cached)
        metadata = {
            k: v for k, v in data.items() if k in {"batch", "cursors", "destination", "blocked"}
        }
        if cache_key:
            metadata["cache_key"] = cache_key
        metadata.update(commit=str(uuid4()), segments=[d for d, _ in segments])
        await self.verified_save(self.store, metadata)
        self.garbage.update(d["key"] for d, _ in self.segments)
        if self.cache_key:
            self.garbage.add(self.cache_key)
        self.cache_key = cache_key
        self.cached = cached
        self.segments = segments
        self.data = data
        await self.cleanup()

    async def bind(self, destination: str) -> None:
        async with self.lock:
            previous = self.data.get("destination")
            if previous is not None and previous != destination:
                if self.data["rows"] or self.data["batch"] is not None:
                    raise HomeAssistantError(
                        "Pending sensor storage belongs to another destination"
                    )
                await self.save(
                    {"rows": [], "batch": None, "cursors": {}, "destination": destination}
                )
            if previous is None:
                if (
                    self.data["rows"]
                    or self.data["batch"] is not None
                    or self.data["cursors"]
                    or {"manifest", "snapshot", "cache_key"} & self.data.keys()
                ):
                    raise HomeAssistantError("Nonempty sensor storage has no destination")
                await self.save({**self.data, "destination": destination})

    async def cache(self, **values) -> None:
        require(values.keys() <= {"manifest", "snapshot"})
        async with self.lock:
            await self.save({**self.data, **deepcopy(values)})

    async def append(
        self,
        rows: list[tuple[str, dict]],
        cursor: tuple[str, int] | None = None,
        *,
        shutdown: bool = False,
    ) -> None:
        async with self.lock:
            known = {identity(source, row): row for source, row in self.data["rows"]}
            frozen = self.blocked_identities()
            if batch := self.data["batch"]:
                frozen.update(identity(batch["source"], row) for row in batch["readings"])
            added = []
            replacements = {}
            for source, row in rows:
                validate_row(row)
                key = identity(source, row)
                if key in known:
                    prior = known[key]
                    promote = incomplete_history(prior) and row["mode"] == "live"
                    original = {k: v for k, v in prior.items() if k != "mode"}
                    if original != {k: v for k, v in row.items() if k != "mode"}:

                        def comparable(value):
                            return {k: v for k, v in value.items() if k not in {"mode", "unit"}}

                        incomplete = prior if prior["mode"] == "history" else row
                        live = row if row["mode"] == "live" else prior
                        if not (
                            incomplete["mode"] == "history"
                            and incomplete["unit"] == ""
                            and live["mode"] == "live"
                            and comparable(prior) == comparable(row)
                        ):
                            raise HomeAssistantError("Conflicting sensor report identity")
                        promote = prior["mode"] == "history"
                    if promote:
                        # Missing-unit live reports must reach the server too. The
                        # exact live observation supersedes only unfrozen history,
                        # even when the payload differs solely in transport mode.
                        if key in frozen:
                            raise HomeAssistantError("Conflicting frozen sensor report identity")
                        replacements[key] = deepcopy(row)
                        known[key] = replacements[key]
                    continue
                known[key] = row
                added.append([source, deepcopy(row)])
            # Reserve enough space to flush the bounded RAM buffer during unload.
            capacity = MAX_REPORTS + (MEMORY_REPORTS if shutdown else 0)
            incomplete = sum(incomplete_history(row) for row in known.values())
            if len(known) - incomplete > capacity:
                raise ReportCapacityFull("Sensor update queue capacity reached")
            # Incomplete history cannot consume live delivery capacity.
            if incomplete > MAX_INCOMPLETE_REPORTS:
                raise IncompleteHistoryFull("Incomplete sensor history capacity reached")
            cursors = {**self.data["cursors"]}
            if cursor:
                cursors[cursor[0]] = max(cursors.get(cursor[0], 0), cursor[1])
            if added or replacements or cursors != self.data["cursors"]:
                await self.save(
                    {
                        **self.data,
                        "rows": [
                            [source, replacements.get(identity(source, row), row)]
                            for source, row in [*self.data["rows"], *added]
                        ],
                        "cursors": cursors,
                    }
                )

    async def cursor(self, assignment_id: str, initial: int) -> int:
        return self.data["cursors"].get(assignment_id, initial)

    async def ensure_cursor(self, assignment_id: str, initial: int) -> None:
        async with self.lock:
            if assignment_id not in self.data["cursors"]:
                await self.save(
                    {**self.data, "cursors": {**self.data["cursors"], assignment_id: initial}}
                )

    async def next_batch(self, centre_id: str) -> dict | None:
        async with self.lock:
            if self.data["batch"] is not None:
                return deepcopy(self.data["batch"])
            if not self.data["rows"]:
                return None
            # Do not freeze a valid historical value without the unit needed to interpret it.
            blocked = self.blocked_identities()
            eligible = [
                (origin, row)
                for origin, row in self.data["rows"]
                if not incomplete_history(row) and identity(origin, row) not in blocked
            ]
            if not eligible:
                return None
            source, first = eligible[0]
            rows = [
                row
                for origin, row in eligible
                if origin == source and row["assignmentId"] == first["assignmentId"]
            ][:100]
            batch = {
                "apiVersion": 1,
                "commandId": str(uuid4()),
                "centreId": centre_id,
                "source": source,
                "readings": rows,
            }
            await self.save({**self.data, "batch": batch})
            return deepcopy(batch)

    def blocked_identities(self) -> set[tuple[str, str, str]]:
        return {
            identity(batch["source"], row)
            for descriptor in self.data.get("blocked", [])
            for batch in (self.blocked_batches[descriptor["key"]],)
            for row in batch["readings"]
        }

    async def block(self, command_id: str, reason: str) -> None:
        """Retain a definitively rejected command unchanged; this is never an ACK.

        Blocked rows continue to occupy the same MAX_REPORTS + shutdown reserve.
        Each descriptor owns at least one row, bounding files/batches as well as
        samples. Reaching that total budget pauses capture, not healthy delivery.
        """
        async with self.lock:
            batch = self.data["batch"]
            if batch is None or batch["commandId"] != command_id or reason != "assignment_window":
                raise ValueError("Unverified sensor batch rejection")
            key = f"{self.key}.files/blocked.{command_id}"
            self.garbage.add(key)
            self.blocked_batches[key] = deepcopy(batch)
            await self.verified_save(self.make_store(key), self.blocked_batches[key])
            await self.save(
                {
                    **self.data,
                    "batch": None,
                    "blocked": [
                        *self.data.get("blocked", []),
                        {"key": key, "commandId": command_id, "reason": reason},
                    ],
                }
            )

    def blocked_diagnostics(self) -> dict:
        blocked_reports = len(self.blocked_identities())
        return {
            "blocked_batches": len(self.data.get("blocked", [])),
            "blocked_reports": blocked_reports,
            "blocked_reasons": {"assignment_window": len(self.data.get("blocked", []))},
            "blocked_report_capacity": MAX_REPORTS + MEMORY_REPORTS,
            "blocked_capacity_shared_with_queue": True,
            "pending_delivery_reports": sum(
                not incomplete_history(row) for _, row in self.data["rows"]
            )
            - blocked_reports,
        }

    async def acknowledge(self, command_id: str) -> None:
        async with self.lock:
            batch = self.data["batch"]
            if batch is None or batch["commandId"] != command_id:
                raise ValueError("Unexpected acknowledgement")
            accepted = {identity(batch["source"], row) for row in batch["readings"]}
            rows = [item for item in self.data["rows"] if identity(*item) not in accepted]
            await self.save({**self.data, "batch": None, "rows": rows})

    async def size(self) -> int:
        return len(self.data["rows"])

    async def deliverable_size(self) -> int:
        """Occupied delivery budget, including blocked rows until explicitly resolved."""
        return sum(not incomplete_history(row) for _, row in self.data["rows"])
