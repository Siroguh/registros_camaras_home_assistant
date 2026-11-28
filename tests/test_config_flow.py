"""Current configuration entries and native Better Auth credentials."""

from hashlib import sha256
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.registros_camaras.api import (
    AuthenticationError,
    CannotConnect,
    IncompatibleApi,
    Snapshot,
)
from custom_components.registros_camaras.const import DEFAULT_CENTRE, DOMAIN
from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResultType

DATA = {"url": "https://registros.example/", "api_key": "native-key", "centre_id": DEFAULT_CENTRE}


async def test_user_and_duplicate(hass, client):
    with patch("custom_components.registros_camaras.async_setup_entry", return_value=True):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
        assert result["type"] is FlowResultType.FORM
        result = await hass.config_entries.flow.async_configure(result["flow_id"], DATA)
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["data"]["api_key"] == "native-key"
        assert result["data"]["url"] == "https://registros.example"
        await hass.async_block_till_done()
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}, data=DATA
        )
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "already_configured"


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (AuthenticationError(), "invalid_auth"),
        (CannotConnect(), "cannot_connect"),
        (IncompatibleApi(), "incompatible_api"),
    ],
)
async def test_flow_errors(hass, client, error, code):
    client.side_effect = error
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}, data=DATA
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": code}


async def test_server_without_sensor_stream_is_rejected_during_configuration(
    hass, client, stream_manifest
):
    stream_manifest.side_effect = IncompatibleApi("Sensor stream unavailable")
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "user"}, data=DATA
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "incompatible_api"}


async def test_unsupported_config_entry_is_not_adopted(hass, client):
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        unique_id="unsupported-entry",
        data={"url": "https://registros.example", "api_key": "test-key"},
    )
    entry.add_to_hass(hass)
    original = dict(entry.data)
    assert not await hass.config_entries.async_setup(entry.entry_id)
    assert entry.state is config_entries.ConfigEntryState.MIGRATION_ERROR
    assert entry.version == 1 and entry.data == original
    client.assert_not_called()


async def test_reauth_replaces_current_credentials_and_drops_unknown_settings(hass, entry, client):
    hass.config_entries.async_update_entry(entry, data={**entry.data, "unused_setting": "discard"})
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_REAUTH, "entry_id": entry.entry_id},
        data=entry.data,
    )
    assert {key.schema for key in result["data_schema"].schema} == {"api_key"}
    with patch("custom_components.registros_camaras.async_setup_entry", return_value=True):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"api_key": "  new-native-key  "}
        )
        assert result["type"] is FlowResultType.ABORT
        assert result["reason"] == "reauth_successful"
        assert entry.data == {
            "url": "https://registros.example",
            "api_key": "new-native-key",
            "centre_id": DEFAULT_CENTRE,
        }
        assert (
            entry.unique_id
            == sha256(f"https://registros.example|{DEFAULT_CENTRE}".encode()).hexdigest()
        )
        await hass.async_block_till_done()


async def test_reconfigure_preserves_key_and_replaces_data(hass, entry, client):
    hass.config_entries.async_update_entry(entry, data={**entry.data, "unused_setting": "discard"})
    with patch("custom_components.registros_camaras.async_setup_entry", return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "reconfigure", "entry_id": entry.entry_id},
            data={"url": "https://new.example/", "api_key": "  "},
        )
        assert result["reason"] == "reconfigure_successful"
        assert entry.data == {
            "url": "https://new.example",
            "api_key": "test-key",
            "centre_id": DEFAULT_CENTRE,
        }
        await hass.async_block_till_done()


async def test_reconfigure_moves_identity_and_rejects_duplicate_destination(hass, client):
    with patch("custom_components.registros_camaras.async_setup_entry", return_value=True):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}, data=DATA
        )
        entry = result["result"]
        await hass.async_block_till_done()
        changed = {**DATA, "url": "https://other.example"}
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "reconfigure", "entry_id": entry.entry_id}, data=changed
        )
        assert result["reason"] == "reconfigure_successful"
        await hass.async_block_till_done()
        duplicate = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}, data=changed
        )
        assert duplicate["reason"] == "already_configured"
        released = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}, data=DATA
        )
        assert released["type"] is FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done()
        blocked = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "reconfigure", "entry_id": entry.entry_id}, data=DATA
        )
        assert blocked["reason"] == "already_configured"
        assert entry.data["url"] == changed["url"]


async def test_pending_batch_cannot_follow_reconfigured_destination(
    hass, entry, client, stream_manifest
):
    from unittest.mock import AsyncMock

    from custom_components.registros_camaras.update_store import UpdateStore

    hass.config.external_url = "https://home.example"
    stream_manifest.return_value["assignments"] = [
        {
            "id": "a",
            "source": "https://home.example",
            "entityId": "sensor.input",
            "locationId": "cam-1",
            "metric": "temperature",
            "position": 1,
            "startsAt": 0,
            "endsAt": None,
            "historyFrom": 0,
        }
    ]
    with patch(
        "custom_components.registros_camaras.api.RegistrosClient.push_updates", new=AsyncMock()
    ) as push:
        assert await hass.config_entries.async_setup(entry.entry_id)
        hass.states.async_set("sensor.input", "20", {"unit_of_measurement": "°C"})
        await hass.async_block_till_done()
        original = await entry.runtime_data.updates.spool.next_batch(DEFAULT_CENTRE)
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "reconfigure", "entry_id": entry.entry_id},
            data={**DATA, "url": "https://other.example"},
        )
        assert result["reason"] == "reconfigure_successful"
        await hass.async_block_till_done()
        assert entry.state is config_entries.ConfigEntryState.SETUP_RETRY
        push.assert_not_called()
        retained = UpdateStore(hass, entry.entry_id)
        await retained.initialize()
        assert await retained.next_batch(DEFAULT_CENTRE) == original
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": "reconfigure", "entry_id": entry.entry_id},
            data={**DATA, "api_key": "rotated-key"},
        )
        assert result["reason"] == "reconfigure_successful"
        await hass.async_block_till_done()
        assert entry.state is config_entries.ConfigEntryState.LOADED
        assert await entry.runtime_data.updates.spool.next_batch(DEFAULT_CENTRE) == original
        assert await hass.config_entries.async_unload(entry.entry_id)


async def test_empty_queue_reconfigure_reloads_and_resets_destination_cache(hass, entry, client):
    assert await hass.config_entries.async_setup(entry.entry_id)
    spool = entry.runtime_data.updates.spool
    await spool.ensure_cursor("old-assignment", 123)
    old_destination = spool.data["destination"]
    client.return_value = Snapshot({**client.return_value.raw, "revision": 99})
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "reconfigure", "entry_id": entry.entry_id},
        data={**DATA, "url": "https://new.example"},
    )
    assert result["reason"] == "reconfigure_successful"
    await hass.async_block_till_done()
    assert entry.state is config_entries.ConfigEntryState.LOADED
    spool = entry.runtime_data.updates.spool
    assert spool.data["destination"] != old_destination
    assert spool.data["cursors"] == {}
    assert spool.data["snapshot"]["revision"] == 99
    assert await hass.config_entries.async_unload(entry.entry_id)
