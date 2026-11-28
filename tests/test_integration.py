"""Real HA setup, entities, availability, services and reloads."""

from copy import deepcopy
from unittest.mock import AsyncMock, patch

from aiohttp import web
import pytest
from pytest_homeassistant_custom_component.common import MockUser

from custom_components.registros_camaras.api import MOVEMENT_ERRORS, CannotConnect, decode_snapshot
from custom_components.registros_camaras.const import DEFAULT_CENTRE, DOMAIN
from custom_components.registros_camaras.diagnostics import async_get_config_entry_diagnostics
from homeassistant.core import Context
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er


def entity_id(hass, entry, platform, location, key):
    return er.async_get(hass).async_get_entity_id(
        platform, DOMAIN, f"{entry.entry_id}_{location}_{key}"
    )


async def setup(hass, entry):
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def test_entities_fail_closed_and_discover_new_chambers(hass, entry, client, raw):
    await setup(hass, entry)
    quantity = entity_id(hass, entry, "sensor", "cam-1", "quantity")
    assert hass.states.get(quantity).state == "45"
    temp = entity_id(hass, entry, "sensor", "cam-1", "temperature_min")
    assert hass.states.get(temp).state == "2"
    assert hass.states.get(temp).attributes["unit_of_measurement"] == "°C"
    assert hass.states.get(entity_id(hass, entry, "sensor", "centre", "due_tasks")).state == "1"
    assert (
        hass.states.get(entity_id(hass, entry, "binary_sensor", "cam-1", "proposal_overdue")).state
        == "on"
    )
    coordinator = entry.runtime_data
    pending = deepcopy(raw)
    pending.update(status="pending", chambers=[], tasks=[])
    client.return_value = decode_snapshot(pending, DEFAULT_CENTRE)
    await coordinator.async_refresh()
    assert hass.states.get(quantity).state == "unavailable"
    client.side_effect = CannotConnect("Disconnected")
    await coordinator.async_refresh()
    assert hass.states.get(quantity).state == "unavailable"
    assert (
        hass.states.get(entity_id(hass, entry, "binary_sensor", "centre", "connection")).state
        == "off"
    )
    client.side_effect = None
    raw["locations"].append({**raw["locations"][0], "id": "cam-2", "name": "Cámara 2"})
    client.return_value = decode_snapshot(raw, DEFAULT_CENTRE)
    await coordinator.async_refresh()
    await hass.async_block_till_done()
    assert hass.states.get(entity_id(hass, entry, "sensor", "cam-2", "quantity")).state == "0"
    raw["locations"][0]["archived"] = True
    client.return_value = decode_snapshot(raw, DEFAULT_CENTRE)
    await coordinator.async_refresh()
    assert hass.states.get(quantity).state == "unavailable"
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert hass.services.has_service(DOMAIN, "confirm_proposal")


async def test_services_preserve_receipt_and_retry_identity(hass, entry, client, receipt, raw):
    await setup(hass, entry)
    command = {
        "config_entry_id": entry.entry_id,
        "command_id": "00000000-0000-4000-8000-000000000123",
        "expected_revision": 7,
        "location_id": "cam-1",
        "proposal_id": "proposal:7",
    }
    with patch.object(
        entry.runtime_data.client, "command", new=AsyncMock(return_value=receipt)
    ) as send:
        client.side_effect = CannotConnect("Poll failed after acceptance")
        for _ in range(2):
            result = await hass.services.async_call(
                DOMAIN, "confirm_proposal", command, blocking=True, return_response=True
            )
            assert result == receipt
        assert send.call_args_list[0] == send.call_args_list[1]
        assert send.call_args.args[1]["expected_revision"] == 7
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "get_snapshot",
            {"config_entry_id": "missing"},
            blocking=True,
            return_response=True,
        )
    client.side_effect = None
    result = await hass.services.async_call(
        DOMAIN,
        "get_snapshot",
        {"config_entry_id": entry.entry_id},
        blocking=True,
        return_response=True,
    )
    assert result["revision"] == 7
    diagnostic = await async_get_config_entry_diagnostics(hass, entry)
    assert "test-key" not in str(diagnostic) and "registros.example" not in str(diagnostic)
    assert raw["chambers"][0]["occupants"][0]["lotId"] not in str(diagnostic)
    old_id = entity_id(hass, entry, "sensor", "cam-1", "quantity")
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entity_id(hass, entry, "sensor", "cam-1", "quantity") == old_id


async def test_native_service_rejects_unverified_business_receipt(hass, entry, client, receipt):
    await setup(hass, entry)
    client.reset_mock()
    command = {
        "config_entry_id": entry.entry_id,
        "command_id": receipt["commandId"],
        "expected_revision": 7,
        "location_id": "cam-1",
        "proposal_id": "proposal:7",
    }
    with patch.object(
        entry.runtime_data.client, "request", new=AsyncMock(return_value={"version": 8})
    ) as request:
        with pytest.raises(HomeAssistantError, match="Invalid command receipt"):
            await hass.services.async_call(
                DOMAIN, "confirm_proposal", command, blocking=True, return_response=True
            )
        client.assert_not_called()
        request.return_value = receipt
        assert (
            await hass.services.async_call(
                DOMAIN, "confirm_proposal", command, blocking=True, return_response=True
            )
            == receipt
        )
        assert request.call_args_list[0] == request.call_args_list[1]
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize(
    "status,payload,code",
    [
        pytest.param(
            409,
            {"_tag": "MovementRejected", "code": "before_entry", "message": "private-detail"},
            "service_validation_error",
            id="translated-movement-rejection",
        ),
        pytest.param(
            409,
            {"_tag": "RevisionConflict", "expectedRevision": 7, "actualRevision": 8},
            "service_validation_error",
            id="revision-conflict",
        ),
        pytest.param(400, {}, "service_validation_error", id="bad-request"),
        pytest.param(422, {}, "service_validation_error", id="business-validation"),
        pytest.param(429, {}, "home_assistant_error", id="rate-limited"),
        pytest.param(500, {}, "home_assistant_error", id="server-failure"),
        pytest.param(503, {}, "home_assistant_error", id="unavailable-server"),
        pytest.param(200, {"version": 8}, "home_assistant_error", id="incomplete-receipt"),
        pytest.param(
            200,
            {
                "commandId": "00000000-0000-4000-8000-000000000999",
                "eventId": "production-fact:confirmed",
                "stream": f"ProductionCentre/{DEFAULT_CENTRE}",
                "version": 8,
            },
            "home_assistant_error",
            id="unmatched-receipt",
        ),
    ],
)
async def test_native_websocket_command_errors_preserve_explicit_retry_identity(
    hass, entry, client, receipt, aiohttp_client, hass_ws_client, status, payload, code
):
    received = []

    async def route(request):
        received.append(await request.json())
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_post("/api/production/{centre}/lots/{lot}/movements", route)
    http = await aiohttp_client(app)
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, "url": str(http.make_url("/"))}
    )
    await setup(hass, entry)
    client.reset_mock()
    websocket = await hass_ws_client(hass)
    command = {
        "config_entry_id": entry.entry_id,
        "command_id": receipt["commandId"],
        "expected_revision": 7,
        "lot_id": "00000000-0000-4000-8000-000000000101",
        "occurred_on": "2026-11-27",
        "source_location_id": "cam-1",
        "destinations": [{"locationId": "cam-2", "quantity": 1}],
    }
    for request_id in (1, 2):
        # Each attempt is explicitly initiated by the caller with its original UUID/payload.
        await websocket.send_json(
            {
                "id": request_id,
                "type": "call_service",
                "domain": DOMAIN,
                "service": "create_movement",
                "service_data": command,
                "return_response": True,
            }
        )
        response = await websocket.receive_json()
        assert response["id"] == request_id
        assert response["success"] is False
        assert response["error"]["code"] == code
        if payload.get("_tag") == "MovementRejected":
            assert MOVEMENT_ERRORS[payload["code"]] in response["error"]["message"]
            assert payload["message"] not in response["error"]["message"]
        await hass.async_block_till_done()
        assert len(received) == request_id
        client.assert_not_called()
    assert received[0] == received[1]
    assert received[0]["commandId"] == command["command_id"]
    assert received[0]["expectedRevision"] == command["expected_revision"]
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_native_websocket_lost_receipt_remains_uncertain_until_explicit_retry(
    hass, entry, client, receipt, aiohttp_client, hass_ws_client
):
    received = []

    async def route(request):
        received.append(await request.json())
        if len(received) == 1:
            # The command has reached the server, but its response never reaches HA.
            request.transport.close()
        return web.json_response(receipt)

    app = web.Application()
    app.router.add_post("/api/centres/{centre}/plans", route)
    http = await aiohttp_client(app)
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, "url": str(http.make_url("/"))}
    )
    await setup(hass, entry)
    client.reset_mock()
    websocket = await hass_ws_client(hass)
    call = {
        "type": "call_service",
        "domain": DOMAIN,
        "service": "confirm_proposal",
        "service_data": {
            "config_entry_id": entry.entry_id,
            "command_id": receipt["commandId"],
            "expected_revision": 7,
            "location_id": "cam-1",
            "proposal_id": "proposal:7",
        },
        "return_response": True,
    }
    await websocket.send_json({"id": 1, **call})
    response = await websocket.receive_json()
    assert response["success"] is False
    assert response["error"]["code"] == "home_assistant_error"
    await hass.async_block_till_done()
    assert len(received) == 1
    client.assert_not_called()
    client.side_effect = CannotConnect("Refresco pendiente")
    await websocket.send_json({"id": 2, **call})
    response = await websocket.receive_json()
    assert response["success"] is True
    assert response["result"]["response"] == receipt
    await hass.async_block_till_done()
    assert len(received) == 2
    assert received[0] == received[1]
    assert received[0]["commandId"] == receipt["commandId"]
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_admin_gate_rejects_reads_and_writes_before_client(hass, entry, client):
    await setup(hass, entry)
    user = MockUser().add_to_hass(hass)
    client.reset_mock()
    with patch.object(entry.runtime_data.client, "command", new=AsyncMock()) as send:
        for service, data in [
            ("get_snapshot", {}),
            (
                "create_lot",
                {
                    "command_id": "00000000-0000-4000-8000-000000000123",
                    "expected_revision": 7,
                    "code": "A",
                    "quantity": 2,
                    "started_on": "2026-11-27",
                },
            ),
        ]:
            with pytest.raises(HomeAssistantError):
                await hass.services.async_call(
                    DOMAIN,
                    service,
                    {"config_entry_id": entry.entry_id, **data},
                    blocking=True,
                    return_response=True,
                    context=Context(user_id=user.id),
                )
        send.assert_not_called()
    client.assert_not_called()


async def test_actions_exist_when_first_connection_fails(hass, entry, client):
    client.side_effect = CannotConnect("Offline")
    assert not await hass.config_entries.async_setup(entry.entry_id)
    assert hass.services.has_service(DOMAIN, "get_snapshot")
    with pytest.raises(HomeAssistantError, match="loaded"):
        await hass.services.async_call(
            DOMAIN,
            "get_snapshot",
            {"config_entry_id": entry.entry_id},
            blocking=True,
            return_response=True,
        )


async def test_task_revision_zero_reaches_api_unchanged(hass, entry, client, receipt):
    await setup(hass, entry)
    with patch.object(
        entry.runtime_data.client, "command", new=AsyncMock(return_value=receipt)
    ) as send:
        await hass.services.async_call(
            DOMAIN,
            "complete_task",
            {
                "config_entry_id": entry.entry_id,
                "command_id": "00000000-0000-4000-8000-000000000123",
                "expected_revision": 7,
                "task_id": "task",
                "task_revision": 0,
                "result": {"kind": "curing_review", "outcome": "ready", "notes": ""},
            },
            blocking=True,
            return_response=True,
        )
        assert send.call_args.args[1]["task_revision"] == 0


async def test_snapshot_cache_bounded_writes_flush_latest_and_restore_offline(
    hass, entry, client, raw, stream_manifest
):
    await setup(hass, entry)
    updates = entry.runtime_data.updates
    with patch.object(updates.spool, "cache", wraps=updates.spool.cache) as cache:
        for revision in range(8, 21):
            raw["revision"] = revision
            client.return_value = decode_snapshot(deepcopy(raw), DEFAULT_CENTRE)
            await entry.runtime_data.async_refresh()
            await hass.async_block_till_done()
        assert entry.runtime_data.data.revision == 20
        assert updates.spool.data["snapshot"]["revision"] == 7
        assert cache.await_count == 0
        # Wake the bounded cache writer at its next eligible interval.
        updates.snapshot_next_write = 0
        updates.write_ready.set()
        await hass.async_block_till_done()
        assert updates.spool.data["snapshot"]["revision"] == 20
        assert cache.await_count == 1
        raw["revision"] = 21
        client.return_value = decode_snapshot(deepcopy(raw), DEFAULT_CENTRE)
        await entry.runtime_data.async_refresh()
        await hass.async_block_till_done()
        assert cache.await_count == 1
        assert await hass.config_entries.async_unload(entry.entry_id)
        assert cache.await_count == 2
    assert updates.snapshot_timer is None
    client.side_effect = CannotConnect()
    stream_manifest.side_effect = CannotConnect()
    assert await hass.config_entries.async_setup(entry.entry_id)
    assert entry.runtime_data.data.revision == 21
    assert not entry.runtime_data.last_update_success
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_failed_snapshot_cache_does_not_hide_accepted_command(
    hass, entry, client, raw, receipt
):
    await setup(hass, entry)
    updates = entry.runtime_data.updates
    updates.snapshot_next_write = 0
    raw["revision"] = 8
    client.return_value = decode_snapshot(raw, DEFAULT_CENTRE)
    command = {
        "config_entry_id": entry.entry_id,
        "command_id": "00000000-0000-4000-8000-000000000123",
        "expected_revision": 7,
        "location_id": "cam-1",
        "proposal_id": "proposal:7",
    }
    with (
        patch.object(entry.runtime_data.client, "command", new=AsyncMock(return_value=receipt)),
        patch.object(updates.spool, "cache", side_effect=HomeAssistantError("cache write failed")),
    ):
        result = await hass.services.async_call(
            DOMAIN, "confirm_proposal", command, blocking=True, return_response=True
        )
        await hass.async_block_till_done()
        assert result == receipt
        assert updates.snapshot_pending["revision"] == 8
        assert updates.spool.data["snapshot"]["revision"] == 7
        assert updates.cache_error == "Dashboard cache pending (HomeAssistantError)"
    assert await hass.config_entries.async_unload(entry.entry_id)
    assert updates.spool.data["snapshot"]["revision"] == 8


async def test_native_root_device_id_preserves_entity_identity_on_reload(
    hass, entry, client, caplog
):
    from homeassistant.helpers import device_registry as dr

    await setup(hass, entry)
    registry = dr.async_get(hass)
    root = registry.async_get_device_by_identifier((DOMAIN, entry.entry_id), entry.entry_id)
    chamber = registry.async_get_device_by_identifier(
        (DOMAIN, f"{entry.entry_id}_cam-1"), entry.entry_id
    )
    assert chamber.via_device_id == root.id
    before = entity_id(hass, entry, "sensor", "cam-1", "quantity")
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    assert entity_id(hass, entry, "sensor", "cam-1", "quantity") == before
    assert (
        registry.async_get_device_by_identifier((DOMAIN, entry.entry_id), entry.entry_id).id
        == root.id
    )
    assert "deprecated `via_device`" not in caplog.text
    assert await hass.config_entries.async_unload(entry.entry_id)
