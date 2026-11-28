"""Async client for the versioned Registros Cámaras 2 API. No HA dependencies."""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import math
import re
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import aiohttp


class ApiError(Exception):
    """A safe error, never containing credentials or server response bodies."""


class AuthenticationError(ApiError):
    """The application rejected the API key."""


class CannotConnect(ApiError):
    """Unavailable server or unknown command outcome."""


class IncompatibleApi(ApiError):
    """The server does not implement our contract."""


class Conflict(ApiError):
    """Refresh and review the new revision before issuing a new command."""


class Rejected(ApiError):
    """The server rejected the operation."""


class UpdatesRejected(ApiError):
    """An authenticated Domain response proves this batch was never accepted."""

    def __init__(self, command_id: str, reason: str) -> None:
        super().__init__("Sensor updates definitively rejected")
        self.command_id = command_id
        self.reason = reason


def normalize_url(value: str) -> str:
    """Accept a server root, never embedded credentials, queries or fragments."""
    parts = urlsplit(value.strip())
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or parts.path not in {"", "/"}
    ):
        raise ValueError("Use the HTTP(S) server root")
    _ = parts.port
    return urlunsplit((parts.scheme, parts.netloc.lower(), "", "", ""))


def segment(value: str) -> str:
    return quote(value, safe="")


@dataclass(frozen=True)
class Snapshot:
    """A single authoritative centre revision; pending reads are not empty stock."""

    raw: dict[str, Any]

    @property
    def ready(self) -> bool:
        return self.raw["status"] == "ready"

    @property
    def revision(self) -> int:
        return self.raw["revision"]

    @property
    def locations(self) -> dict[str, dict[str, Any]]:
        return {v["id"]: v for v in self.raw["locations"] if v["kind"] != "terminal"}

    @property
    def chambers(self) -> dict[str, dict[str, Any]]:
        return {v["locationId"]: v for v in self.raw["chambers"]}

    @property
    def tasks(self) -> list[dict[str, Any]]:
        return self.raw["tasks"]


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("Invalid contract value")


def _timestamp(value: Any) -> None:
    _require(type(value) is int and 0 <= value <= 8_640_000_000_000_000)
    datetime.fromtimestamp(value / 1000, UTC)


def decode_sensor_stream(value: Any) -> dict:
    """Validate the current manifest identically over HTTP and from a saved cache."""
    try:
        _require(type(value["apiVersion"]) is int and value["apiVersion"] == 1)
        for key, expected in (("maxBatchSize", 100), ("maxBatchDelayMillis", 2000)):
            _require(type(value[key]) is int and value[key] == expected)
        _require(type(value["revision"]) is int and value["revision"] >= 0)
        _require(isinstance(value["assignments"], list))
        ids = set()
        for assignment in value["assignments"]:
            for key in ("id", "source", "entityId", "locationId"):
                _require(isinstance(assignment[key], str) and bool(assignment[key]))
            _require(assignment["id"] not in ids)
            ids.add(assignment["id"])
            _require(assignment["entityId"].startswith("sensor."))
            _require(assignment["metric"] in {"temperature", "humidity"})
            _require(type(assignment["position"]) is int and assignment["position"] in {1, 2})
            for key in ("startsAt", "historyFrom"):
                _timestamp(assignment[key])
            if assignment["endsAt"] is not None:
                _timestamp(assignment["endsAt"])
        return value
    except (KeyError, TypeError, ValueError, OverflowError, OSError) as err:
        raise IncompatibleApi("Invalid sensor stream manifest") from err


def decode_snapshot(value: Any, centre_id: str) -> Snapshot:
    """Fail closed on malformed snapshots rather than publish plausible zeroes."""
    try:
        _require(type(value["apiVersion"]) is int and value["apiVersion"] == 1)
        _require(value["centreId"] == centre_id)
        _require(value["status"] in {"ready", "pending"})
        for key in ("revision", "planRevision"):
            _require(type(value[key]) is int and value[key] >= 0)
        _require(isinstance(value["sampledAt"], str))
        _require(
            re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
                value["sampledAt"],
            )
            is not None
        )
        datetime.fromisoformat(value["sampledAt"])
        _require(isinstance(value["lotCodes"], dict))
        for lot_id, code in value["lotCodes"].items():
            _require(isinstance(lot_id, str) and isinstance(code, str))
        for key in ("locations", "chambers", "tasks"):
            _require(isinstance(value[key], list))
        for loc in value["locations"]:
            _require(isinstance(loc["id"], str) and isinstance(loc["name"], str))
            _require(loc["centreId"] == centre_id)
            _require(loc["kind"] in {"chamber", "storage", "terminal"})
            _require(type(loc["archived"]) is bool)
        for chamber in value["chambers"]:
            _require(isinstance(chamber["locationId"], str))
            _require(
                chamber["status"] in {"empty", "no_plan", "no_phase", "compatible", "conflict"}
            )
            _require(isinstance(chamber["occupants"], list))
            for occupant in chamber["occupants"]:
                _require(isinstance(occupant["lotId"], str))
                _require(type(occupant["quantity"]) is int and occupant["quantity"] >= 0)
                _require(type(occupant["age"]) is int)
                for key in ("baseVersionId", "privateRevisionId", "stepKey"):
                    _require(occupant[key] is None or isinstance(occupant[key], str))
                _validate_limits(occupant["limits"])
            _require(chamber["automaticId"] is None or isinstance(chamber["automaticId"], str))
            _validate_deviations(chamber["automaticDeviations"])
            if (origin := chamber["automaticOrigin"]) is not None:
                _timestamp(origin["startsAt"])
                _require(origin["replacesId"] is None or isinstance(origin["replacesId"], str))
                _require(isinstance(origin["confirmationCommandId"], str))
                _validate_actor(origin["actor"])
            for key in ("effective", "ideal", "automatic"):
                _validate_limits(chamber[key])
            if chamber["manual"] is not None:
                _validate_limits(chamber["manual"]["limits"])
                _require(chamber["manual"]["limits"] is not None)
                _require(isinstance(chamber["manual"]["id"], str))
                _require(isinstance(chamber["manual"]["reason"], str))
                _validate_actor(chamber["manual"]["actor"])
                _timestamp(chamber["manual"]["startsAt"])
                _timestamp(chamber["manual"]["endsAt"])
            if (proposal := chamber["proposal"]) is not None:
                _require(isinstance(proposal["id"], str))
                _require(isinstance(proposal["contextKey"], str))
                _require(type(proposal["overdue"]) is bool)
                _timestamp(proposal["createdAt"])
                _timestamp(proposal["dueAt"])
                _validate_limits(proposal["limits"])
                _validate_deviations(proposal["deviations"])
        for task in value["tasks"]:
            _require(isinstance(task["id"], str) and isinstance(task["lotId"], str))
            _require(task["status"] in {"pending", "due"})
            _require(type(task["revision"]) is int and task["revision"] >= 0)
            _timestamp(task["dueAt"])
            _require(task["step"]["kind"] in {"curing_review", "quality_check", "remove_product"})
        return Snapshot(value)
    except (KeyError, TypeError, ValueError, OverflowError, OSError) as err:
        raise IncompatibleApi("Invalid integration snapshot") from err


def _validate_limits(limits: Any) -> None:
    if limits is None:
        return
    for metric in ("temperature", "humidity"):
        bounds = limits[metric]
        _require(type(bounds["min"]) in (int, float) and math.isfinite(bounds["min"]))
        _require(type(bounds["max"]) in (int, float) and math.isfinite(bounds["max"]))
        _require(bounds["min"] <= bounds["max"])
        if metric == "humidity":
            _require(0 <= bounds["min"] <= bounds["max"] <= 100)


def _validate_deviations(deviations: Any) -> None:
    if deviations is None:
        return
    for metric in ("temperature", "humidity"):
        value = deviations[metric]
        _require(type(value) in (int, float) and math.isfinite(value) and value >= 0)


def _validate_actor(actor: Any) -> None:
    _require(actor["type"] in {"user", "integration"})
    _require(isinstance(actor["userId"], str) and bool(actor["userId"]))
    if actor["type"] == "integration":
        _require(isinstance(actor["keyId"], str) and bool(actor["keyId"]))


def decode_command_receipt(value: Any, command_id: str) -> dict:
    """Business fact IDs are independent of stream/version, but bound to this command."""
    try:
        for key in ("commandId", "stream", "eventId"):
            _require(isinstance(value[key], str) and bool(value[key]))
        _require(value["commandId"] == command_id)
        _require(type(value["version"]) is int and value["version"] >= 0)
        return value
    except (KeyError, TypeError, ValueError) as err:
        raise IncompatibleApi("Invalid command receipt") from err


MOVEMENT_ERRORS = {
    "before_entry": "La fecha precede a la entrada inicial del lote.",
    "insufficient_stock": "No hay existencias suficientes en la fecha seleccionada.",
    "ambiguous_sources": "Selecciona las procedencias o activa la mezcla de origen.",
    "invalid_contributions": "Revisa las cantidades y procedencias del reparto.",
    "invalid_distribution": "Revisa las cantidades y destinos del reparto.",
    "immutable_entry": "La entrada inicial se corrige desde la ficha del lote.",
    "archived_destination": "El destino está archivado.",
}


class RegistrosClient:
    def __init__(self, session: aiohttp.ClientSession, url: str, key: str, centre_id: str) -> None:
        self.session = session
        self.url = normalize_url(url)
        self._key = key.strip()
        if not re.fullmatch(r"[a-z0-9-]{1,100}", centre_id):
            raise ValueError("Invalid centre")
        self.centre_id = centre_id

    async def request(
        self, method: str, path: str, body: dict | None = None, params: dict | None = None
    ) -> dict:
        try:
            async with asyncio.timeout(30):
                async with self.session.request(
                    method,
                    f"{self.url}/api/{path}",
                    headers={"Authorization": f"Bearer {self._key}", "Accept": "application/json"},
                    json=body,
                    params=params,
                    allow_redirects=False,
                ) as response:
                    if response.status in {401, 403}:
                        raise AuthenticationError("API key rejected")
                    if (
                        response.status == 422
                        and method == "POST"
                        and path == "environment/updates"
                    ):
                        # Generic HTTP failures cannot prove an unknown command was rejected.
                        # Only this versioned, command-bound Domain contract permits quarantine.
                        payload = bytearray()
                        async for chunk in response.content.iter_chunked(4096):
                            payload.extend(chunk)
                            if len(payload) > 4096:
                                raise IncompatibleApi("Rejection response too large")
                        try:
                            rejection = json.loads(payload)
                        except (ValueError, UnicodeError) as err:
                            raise IncompatibleApi("Invalid rejection response") from err
                        if (
                            isinstance(rejection, dict)
                            and rejection.get("_tag") == "SensorUpdatesRejected"
                            and type(rejection.get("apiVersion")) is int
                            and rejection["apiVersion"] == 1
                            and body is not None
                            and rejection.get("commandId") == body["commandId"]
                            and rejection.get("reason") == "assignment_window"
                            and rejection.get("accepted") is False
                        ):
                            raise UpdatesRejected(body["commandId"], "assignment_window")
                        raise IncompatibleApi("Unverified sensor update rejection")
                    if response.status == 409:
                        try:
                            error = json.loads(await response.content.read(65536))
                        except ValueError, UnicodeError:
                            error = {}
                        if isinstance(error, dict) and error.get("_tag") == "MovementRejected":
                            raise Rejected(
                                MOVEMENT_ERRORS.get(
                                    error.get("code"), "Movimiento rechazado; revisa los datos."
                                )
                            )
                        raise Conflict("Revision changed; refresh and review")
                    if response.status in {404, 405}:
                        raise IncompatibleApi("Endpoint, centre or resource not found")
                    if response.status == 429 or response.status >= 500:
                        raise CannotConnect("Server unavailable; retry commands with the same ID")
                    if not 200 <= response.status < 300:
                        raise Rejected("Operation rejected")
                    payload = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        payload.extend(chunk)
                        if len(payload) > 8 * 1024 * 1024:
                            raise IncompatibleApi("Response too large")
                    result = json.loads(payload)
                    if not isinstance(result, dict):
                        raise IncompatibleApi("Expected an object")
                    return result
        except (aiohttp.ClientError, TimeoutError) as err:
            raise CannotConnect("Connection failed; retry commands with the same ID") from err
        except (ValueError, UnicodeError) as err:
            raise IncompatibleApi("Invalid JSON response") from err

    async def snapshot(self) -> Snapshot:
        return decode_snapshot(
            await self.request("GET", f"integration/{self.centre_id}"), self.centre_id
        )

    async def sensor_stream(self) -> dict:
        return decode_sensor_stream(
            await self.request("GET", f"environment/streams/{self.centre_id}")
        )

    async def push_updates(self, batch: dict) -> None:
        receipt = await self.request("POST", "environment/updates", batch)
        command_id = batch["commandId"]
        stream = receipt.get("stream")
        version = receipt.get("version")
        if (
            receipt.get("commandId") != command_id
            or type(version) is not int
            or stream != f"ReadingTransaction/{command_id}"
            or not 4 <= version <= 103
            or receipt.get("eventId") != f"{stream}:{version}"
        ):
            raise IncompatibleApi("Unexpected durable ingestion acknowledgement")

    async def command(self, action: str, data: dict) -> dict:
        """IDs and revisions come from the caller, so retries remain durable across HA restarts."""
        body = {"commandId": data["command_id"], "expectedRevision": data["expected_revision"]}
        plans_path = f"centres/{self.centre_id}/plans"
        lots_path = f"production/{self.centre_id}/lots"
        if action == "confirm_proposal":
            path = plans_path
            body["action"] = {
                "kind": "confirm",
                "locationId": data["location_id"],
                "proposalId": data["proposal_id"],
            }
        elif action == "complete_task":
            path = f"centres/{self.centre_id}/tasks/{segment(data['task_id'])}/completions"
            body.update(expectedTaskRevision=data["task_revision"], result=data["result"])
        elif action == "create_lot":
            path = lots_path
            body.update(code=data["code"], quantity=data["quantity"], startedOn=data["started_on"])
        elif action in {"schedule_manual", "replace_manual", "finish_manual"}:
            path = plans_path
            change = {"kind": action, "locationId": data["location_id"], "reason": data["reason"]}
            if action != "schedule_manual":
                change["manualId"] = data["manual_id"]
            if action != "finish_manual":
                change.update(
                    limits=data["limits"], startsAt=data["starts_at"], endsAt=data["ends_at"]
                )
            body["action"] = change
        elif action in {"create_movement", "correct_movement"}:
            path = f"{lots_path}/{segment(data['lot_id'])}/movements"
            body.update(
                occurredOn=data["occurred_on"],
                sourceLocationId=data["source_location_id"],
                destinations=data["destinations"],
                mixSources=data.get("mix_sources", False),
                notes=data.get("notes", ""),
            )
            if action == "correct_movement":
                path += f"/{segment(data['movement_id'])}/corrections"
                body["reason"] = data["reason"]
        else:
            raise Rejected("Unknown action")
        return decode_command_receipt(await self.request("POST", path, body), body["commandId"])
