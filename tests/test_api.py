"""Exercise real HTTP transport, credential boundaries and command payloads."""

import json

from aiohttp import web
import pytest

from custom_components.registros_camaras.api import (
    AuthenticationError,
    Conflict,
    IncompatibleApi,
    RegistrosClient,
    Rejected,
    decode_command_receipt,
    decode_sensor_stream,
    decode_snapshot,
    normalize_url,
)
from custom_components.registros_camaras.const import DEFAULT_CENTRE


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@example.com",
        "file:///etc/passwd",
        "https://example.com/api",
        "https://example.com?key=secret",
        "https://example.com/#token",
    ],
)
def test_reject_unsafe_urls(url):
    with pytest.raises(ValueError):
        normalize_url(url)


def test_invalid_snapshot(raw):
    raw["chambers"][0]["occupants"][0]["quantity"] = "unknown"
    with pytest.raises(IncompatibleApi):
        decode_snapshot(raw, DEFAULT_CENTRE)


@pytest.mark.parametrize(
    "path,value",
    [
        (("sampledAt",), "not-a-date"),
        (("sampledAt",), "2026-11-27"),
        (("sampledAt",), "2026-11-27T12:00:00"),
        (("sampledAt",), "2026-02-30T12:00:00Z"),
        (("lotCodes",), []),
        (("lotCodes",), {"lot-1": 12}),
        (("revision",), -1),
        (("planRevision",), True),
        (("tasks", 0, "revision"), -1),
        (("tasks", 0, "revision"), True),
        (("chambers", 0, "occupants"), None),
        (("chambers", 0, "automaticId"), 123),
        (("chambers", 0, "automaticOrigin"), {}),
        (("chambers", 0, "automaticDeviations"), {}),
        (("chambers", 0, "automaticDeviations"), {"temperature": True, "humidity": 0}),
        (("chambers", 0, "proposal", "deviations"), {"temperature": 0, "humidity": -1}),
        (("chambers", 0, "proposal", "createdAt"), -1),
        (("chambers", 0, "proposal", "createdAt"), True),
    ],
)
def test_malformed_snapshot_fields_fail_closed(raw, path, value):
    target = raw
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(IncompatibleApi):
        decode_snapshot(raw, DEFAULT_CENTRE)


@pytest.mark.parametrize(
    "path",
    [
        ("lotCodes",),
        ("chambers", 0, "occupants"),
        ("chambers", 0, "automaticId"),
        ("chambers", 0, "automaticOrigin"),
        ("chambers", 0, "automaticDeviations"),
        ("chambers", 0, "proposal", "deviations"),
        ("chambers", 0, "occupants", 0, "limits"),
        ("chambers", 0, "proposal", "createdAt"),
    ],
)
def test_snapshot_required_fields_cannot_be_defaulted(raw, path):
    target = raw
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    with pytest.raises(IncompatibleApi):
        decode_snapshot(raw, DEFAULT_CENTRE)


def test_manual_reason_required_and_nullable_automatic_origin(raw):
    chamber = raw["chambers"][0]
    chamber.update(automaticId=None, automaticOrigin=None)
    chamber["manual"] = {
        "id": "00000000-0000-4000-8000-000000000105",
        "limits": chamber["effective"],
        "startsAt": 1,
        "endsAt": 2,
        "actor": {"type": "user", "userId": "operator"},
        "reason": "Inspection",
    }
    assert decode_snapshot(raw, DEFAULT_CENTRE).ready
    del chamber["manual"]["reason"]
    with pytest.raises(IncompatibleApi):
        decode_snapshot(raw, DEFAULT_CENTRE)


@pytest.fixture
def manifest():
    return {
        "apiVersion": 1,
        "revision": 0,
        "maxBatchSize": 100,
        "maxBatchDelayMillis": 2000,
        "assignments": [
            {
                "id": "assignment",
                "source": "https://home.example",
                "entityId": "sensor.temperature",
                "locationId": "cam-1",
                "metric": "temperature",
                "position": 1,
                "startsAt": 0,
                "endsAt": None,
                "historyFrom": 0,
            }
        ],
    }


@pytest.mark.parametrize(
    "key,value",
    [
        ("metric", "pressure"),
        ("metric", None),
        ("position", 0),
        ("position", 3),
        ("position", True),
        ("position", 1.0),
        ("startsAt", -1),
        ("startsAt", True),
        ("startsAt", 1.5),
        ("endsAt", -1),
        ("endsAt", False),
        ("endsAt", 1.0),
        ("historyFrom", -1),
        ("historyFrom", True),
        ("historyFrom", "1"),
    ],
)
def test_manifest_decoder_rejects_invalid_assignment_fields(manifest, key, value):
    manifest["assignments"][0][key] = value
    with pytest.raises(IncompatibleApi):
        decode_sensor_stream(manifest)


@pytest.mark.parametrize("key", ["metric", "position", "startsAt", "endsAt", "historyFrom"])
def test_manifest_decoder_requires_current_assignment_fields(manifest, key):
    del manifest["assignments"][0][key]
    with pytest.raises(IncompatibleApi):
        decode_sensor_stream(manifest)


@pytest.mark.parametrize("metric,position", [("temperature", 1), ("humidity", 2)])
async def test_sensor_manifest_http_and_cache_use_same_contract(
    aiohttp_client, socket_enabled, manifest, metric, position
):
    manifest["assignments"][0].update(metric=metric, position=position)

    async def route(request):
        return web.json_response(manifest)

    app = web.Application()
    app.router.add_get("/api/environment/streams/" + DEFAULT_CENTRE, route)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", DEFAULT_CENTRE)
    assert await client.sensor_stream() == decode_sensor_stream(manifest)
    manifest["assignments"][0]["position"] = True
    with pytest.raises(IncompatibleApi):
        await client.sensor_stream()


@pytest.mark.parametrize("key", ["commandId", "eventId", "stream", "version"])
def test_business_receipt_requires_every_field(receipt, key):
    command_id = receipt["commandId"]
    del receipt[key]
    with pytest.raises(IncompatibleApi):
        decode_command_receipt(receipt, command_id)


@pytest.mark.parametrize(
    "change",
    [
        {"commandId": "00000000-0000-4000-8000-000000000999"},
        {"commandId": None},
        {"eventId": ""},
        {"eventId": 123},
        {"stream": ""},
        {"stream": None},
        {"version": True},
        {"version": -1},
        {"version": 8.0},
        {"version": "8"},
    ],
)
def test_business_receipt_invalid_fields(receipt, change):
    with pytest.raises(IncompatibleApi):
        decode_command_receipt({**receipt, **change}, receipt["commandId"])


@pytest.mark.parametrize(
    "action",
    [
        "confirm_proposal",
        "complete_task",
        "create_lot",
        "schedule_manual",
        "replace_manual",
        "finish_manual",
        "create_movement",
        "correct_movement",
    ],
)
async def test_every_business_command_validates_its_receipt(
    aiohttp_client, socket_enabled, receipt, action
):
    returned = receipt.copy()

    async def route(request):
        assert (await request.json())["commandId"] == receipt["commandId"]
        return web.json_response(returned)

    app = web.Application()
    app.router.add_post("/api/{tail:.*}", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", DEFAULT_CENTRE)
    data = {
        "command_id": receipt["commandId"],
        "expected_revision": 7,
        "location_id": "cam-1",
        "proposal_id": "proposal:7",
        "task_id": "task:1",
        "task_revision": 0,
        "result": {
            "kind": "quality_check",
            "outcome": "passed",
            "sampleSize": 1,
            "nonconforming": 0,
            "notes": "",
        },
        "code": "A",
        "quantity": 1,
        "started_on": "2026-11-27",
        "reason": "Inspection",
        "manual_id": "00000000-0000-4000-8000-000000000105",
        "limits": {"temperature": {"min": 2, "max": 6}, "humidity": {"min": 50, "max": 70}},
        "starts_at": 1795777200000,
        "ends_at": 1795777500000,
        "lot_id": "00000000-0000-4000-8000-000000000101",
        "occurred_on": "2026-11-27",
        "source_location_id": "cam-1",
        "destinations": [{"locationId": "cam-2", "quantity": 1}],
        "movement_id": "00000000-0000-4000-8000-000000000106",
    }
    # Facts may have an ID independent of the stream and revision.
    assert receipt["eventId"] != f"{receipt['stream']}:{receipt['version']}"
    assert await client.command(action, data) == receipt
    returned["commandId"] = "00000000-0000-4000-8000-000000000999"
    with pytest.raises(IncompatibleApi):
        await client.command(action, data)
    returned.clear()
    returned["version"] = 8
    with pytest.raises(IncompatibleApi):
        await client.command(action, data)


def test_storage_locations_keep_dashboard_entities(raw):
    raw["locations"].extend(
        [
            {
                "id": "storage-108",
                "centreId": DEFAULT_CENTRE,
                "name": "Producto terminado",
                "kind": "storage",
                "archived": False,
            },
            {
                "id": "finalizado",
                "centreId": DEFAULT_CENTRE,
                "name": "Finalizado",
                "kind": "terminal",
                "archived": False,
            },
        ]
    )
    snapshot = decode_snapshot(raw, DEFAULT_CENTRE)
    assert "storage-108" in snapshot.locations
    assert "finalizado" not in snapshot.locations


async def test_http_contract_and_exact_retries(aiohttp_client, socket_enabled, raw, receipt):
    requests = []

    async def route(request):
        assert request.headers["Authorization"] == "Bearer test-native-key"
        if request.method == "GET":
            return web.json_response(raw)
        requests.append((request.path, await request.json()))
        return web.json_response(receipt)

    app = web.Application()
    app.router.add_route("*", "/api/{tail:.*}", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(
        http.session, str(http.make_url("/")).rstrip("/"), "test-native-key", DEFAULT_CENTRE
    )
    snapshot = await client.snapshot()
    assert snapshot.ready and snapshot.revision == 7
    command = {
        "command_id": "00000000-0000-4000-8000-000000000123",
        "expected_revision": 7,
        "location_id": "cam-1",
        "proposal_id": "proposal:7",
    }
    for _ in range(2):
        await client.command("confirm_proposal", command)
    assert requests[0] == requests[1]
    assert requests[0][1] == {
        "commandId": command["command_id"],
        "expectedRevision": 7,
        "action": {"kind": "confirm", "locationId": "cam-1", "proposalId": "proposal:7"},
    }
    await client.command(
        "complete_task",
        {
            **command,
            "task_id": "task:1/a",
            "task_revision": 3,
            "result": {"kind": "curing_review", "outcome": "ready", "notes": ""},
        },
    )
    assert requests[-1][1]["expectedTaskRevision"] == 3
    await client.command(
        "create_lot", {**command, "code": "A", "quantity": 10, "started_on": "2026-11-27"}
    )
    assert requests[-1][1]["startedOn"] == "2026-11-27"
    await client.command(
        "create_movement",
        {
            **command,
            "lot_id": "00000000-0000-4000-8000-000000000101",
            "source_location_id": "cam-1",
            "occurred_on": "2026-11-27",
            "destinations": [{"locationId": "cam-2", "quantity": 5}],
        },
    )
    assert requests[-1][1]["destinations"][0]["quantity"] == 5


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (401, AuthenticationError),
        (403, AuthenticationError),
        (409, Conflict),
        (404, IncompatibleApi),
        (302, Rejected),
    ],
)
async def test_error_and_redirect_boundaries(aiohttp_client, socket_enabled, status, error):
    async def route(request):
        return web.Response(
            status=status,
            headers={"Location": "https://other.example"},
            text="private server details",
        )

    app = web.Application()
    app.router.add_get("/api/integration/" + DEFAULT_CENTRE, route)
    http = await aiohttp_client(app)
    client = RegistrosClient(
        http.session, str(http.make_url("/")).rstrip("/"), "test-key", DEFAULT_CENTRE
    )
    with pytest.raises(error) as caught:
        await client.snapshot()
    assert "private server details" not in str(caught.value)


def test_translation_keys_and_service_descriptions():
    from pathlib import Path

    import yaml

    from custom_components.registros_camaras.services import SCHEMAS

    root = Path(__file__).parents[1] / "custom_components/registros_camaras"
    strings = json.loads((root / "strings.json").read_text())
    assert strings == json.loads((root / "translations/en.json").read_text())
    spanish = json.loads((root / "translations/es.json").read_text())

    def keys(value, prefix=()):
        return {
            path
            for key, child in value.items()
            for path in (
                keys(child, (*prefix, key)) if isinstance(child, dict) else {(*prefix, key)}
            )
        }

    assert keys(spanish) == keys(strings)
    descriptions = yaml.safe_load((root / "services.yaml").read_text())
    assert descriptions.keys() == strings["services"].keys() == SCHEMAS.keys()
    for name, schema in SCHEMAS.items():
        assert descriptions[name]["fields"].keys() == {key.schema for key in schema}
        assert strings["services"][name]["fields"].keys() == descriptions[name]["fields"].keys()


async def test_business_rejection_is_not_a_stale_revision(aiohttp_client, socket_enabled):
    async def route(request):
        return web.json_response(
            {"_tag": "MovementRejected", "code": "before_entry", "message": "private detail"},
            status=409,
        )

    app = web.Application()
    app.router.add_get("/api/options", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(
        http.session, str(http.make_url("/")).rstrip("/"), "test-key", DEFAULT_CENTRE
    )
    with pytest.raises(Rejected, match="fecha precede") as caught:
        await client.request("GET", "options")
    assert "private detail" not in str(caught.value)


@pytest.mark.parametrize("value", [2.9, True, "2.9", float("inf"), 9007199254740992])
def test_command_integer_conversion_never_truncates(value):
    import probatio as vol

    from custom_components.registros_camaras.services import SCHEMAS

    data = {
        "config_entry_id": "entry",
        "command_id": "00000000-0000-4000-8000-000000000123",
        "expected_revision": 7,
        "code": "A",
        "quantity": 2,
        "started_on": "2026-11-27",
    }
    for key in ("quantity", "expected_revision"):
        with pytest.raises(vol.Invalid):
            vol.Schema(SCHEMAS["create_lot"])({**data, key: value})


@pytest.mark.parametrize("end", [None, "tomorrow", float("inf"), 10**30])
def test_invalid_manual_end_is_rejected_before_entity_update(raw, end):
    raw["chambers"][0]["manual"] = {
        "id": "00000000-0000-4000-8000-000000000105",
        "startsAt": 1,
        "endsAt": end,
        "limits": raw["chambers"][0]["effective"],
        "reason": "Inspection",
        "actor": {"type": "user", "userId": "operator"},
    }
    with pytest.raises(IncompatibleApi):
        decode_snapshot(raw, DEFAULT_CENTRE)


def test_validation_survives_python_optimization(raw):
    import subprocess
    import sys

    raw["apiVersion"] = 2
    script = """
import json, sys
from custom_components.registros_camaras.api import decode_snapshot, IncompatibleApi
value = json.load(sys.stdin)
try:
    decode_snapshot(value, 'villarroya-01-01')
except IncompatibleApi:
    sys.exit(0)
sys.exit(1)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        input=json.dumps(raw),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "change,expected",
    [
        ({}, "blocked"),
        ({"commandId": "another-command"}, "pending"),
        ({"accepted": True}, "pending"),
        ({"accepted": 0}, "pending"),
        ({"apiVersion": True}, "pending"),
        ({"reason": "private response detail"}, "pending"),
        ({"_tag": "ProcessInvalid"}, "pending"),
    ],
)
async def test_only_command_bound_domain_rejection_can_block_updates(
    aiohttp_client, socket_enabled, change, expected
):
    from custom_components.registros_camaras.api import UpdatesRejected

    command_id = "00000000-0000-4000-8000-000000000123"
    rejection = {
        "_tag": "SensorUpdatesRejected",
        "apiVersion": 1,
        "commandId": command_id,
        "reason": "assignment_window",
        "accepted": False,
        **change,
    }

    async def route(request):
        assert request.headers["Authorization"] == "Bearer test-key"
        assert (await request.json())["commandId"] == command_id
        return web.json_response(rejection, status=422)

    app = web.Application()
    app.router.add_post("/api/environment/updates", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", DEFAULT_CENTRE)
    error = UpdatesRejected if expected == "blocked" else IncompatibleApi
    with pytest.raises(error) as caught:
        await client.push_updates({"commandId": command_id})
    assert "private response detail" not in str(caught.value)
    if expected == "blocked":
        assert caught.value.command_id == command_id
        assert caught.value.reason == "assignment_window"


@pytest.mark.parametrize("status", [400, 409, 500])
async def test_untrusted_status_never_proves_batch_rejection(
    aiohttp_client, socket_enabled, status
):
    from custom_components.registros_camaras.api import ApiError, UpdatesRejected

    async def route(request):
        return web.json_response(
            {
                "_tag": "SensorUpdatesRejected",
                "apiVersion": 1,
                "commandId": "id",
                "reason": "assignment_window",
                "accepted": False,
            },
            status=status,
        )

    app = web.Application()
    app.router.add_post("/api/environment/updates", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", DEFAULT_CENTRE)
    with pytest.raises(ApiError) as caught:
        await client.push_updates({"commandId": "id"})
    assert not isinstance(caught.value, UpdatesRejected)


@pytest.mark.parametrize(
    "stream_kind,version,change,accepted",
    [
        ("ReadingIngestion", 1, {}, False),
        ("ReadingTransaction", 4, {}, True),
        ("ReadingTransaction", 7, {}, True),
        ("ReadingTransaction", 103, {}, True),
        ("ReadingTransaction", 1, {}, False),
        ("ReadingTransaction", 3, {}, False),
        ("ReadingTransaction", 104, {}, False),
        ("ReadingTransaction", True, {}, False),
        ("ReadingTransaction", 4.0, {}, False),
        ("ReadingTransaction", "4", {}, False),
        ("ReadingIngestion", True, {"eventId": "ReadingIngestion/{id}:1"}, False),
        ("ReadingIngestion", 1.0, {"eventId": "ReadingIngestion/{id}:1"}, False),
        ("ReadingIngestion", 2, {}, False),
        ("ReadingTransactionExtra", 4, {}, False),
        ("OtherStream", 4, {}, False),
        ("ReadingTransaction", 4, {"commandId": "other-command"}, False),
        ("ReadingTransaction", 4, {"stream": "ReadingTransaction/other-command"}, False),
        ("ReadingIngestion", 1, {"stream": "ReadingIngestion/other-command"}, False),
        ("ReadingTransaction", 4, {"eventId": "ReadingTransaction/{id}:5"}, False),
        ("ReadingTransaction", 4, {"eventId": "ReadingTransaction/other-command:4"}, False),
        ("ReadingTransaction", 4, {"eventId": "ReadingIngestion/{id}:4"}, False),
        ("ReadingIngestion", 1, {"eventId": "ReadingTransaction/{id}:1"}, False),
        ("ReadingTransaction", None, {}, False),
    ],
)
async def test_only_final_transaction_receipts_are_exact_and_command_bound(
    aiohttp_client, socket_enabled, stream_kind, version, change, accepted
):
    command_id = "00000000-0000-4000-8000-000000000123"
    receipt = {
        "commandId": command_id,
        "stream": f"{stream_kind}/{command_id}",
        "eventId": f"{stream_kind}/{command_id}:{version}",
        "version": version,
        **{key: value.format(id=command_id) for key, value in change.items()},
    }
    # The client validates the receipt contract, not Domain's family grouping.
    batch = {"commandId": command_id}

    async def route(request):
        assert await request.json() == batch
        return web.json_response(receipt)

    app = web.Application()
    app.router.add_post("/api/environment/updates", route)
    http = await aiohttp_client(app)
    client = RegistrosClient(http.session, str(http.make_url("/")), "test-key", DEFAULT_CENTRE)
    if accepted:
        await client.push_updates(batch)
    else:
        with pytest.raises(IncompatibleApi, match="Unexpected durable ingestion acknowledgement"):
            await client.push_updates(batch)
