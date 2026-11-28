"""Typed HA actions with explicit retry identity and native response data."""

import re

import probatio as vol

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.service import async_register_admin_service

from .api import ApiError, AuthenticationError, Conflict, Rejected, segment
from .const import DOMAIN

ENTRY = {vol.Required("config_entry_id"): cv.string}


def exact_int(value):
    if isinstance(value, bool) or not (
        type(value) is int
        or (type(value) is float and value.is_integer())
        or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value))
    ):
        raise vol.Invalid("Expected an exact integer")
    result = int(value)
    if abs(result) > 9007199254740991:
        raise vol.Invalid("Integer exceeds the API range")
    return result


COMMAND = {
    **ENTRY,
    vol.Required("command_id"): vol.All(
        cv.string,
        vol.Match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
        ),
    ),
    vol.Required("expected_revision"): vol.All(exact_int, vol.Range(min=0)),
}
SCHEMAS = {
    "get_lot": {**ENTRY, vol.Required("lot_id"): cv.string},
    "get_movement_history": {**ENTRY, vol.Required("lot_id"): cv.string},
    "get_snapshot": ENTRY,
    "get_lots": {
        **ENTRY,
        vol.Optional("before"): vol.All(exact_int, vol.Range(min=1)),
        vol.Optional("search"): cv.string,
        vol.Optional("limit", default=50): vol.All(exact_int, vol.In([50, 100])),
    },
    "get_movement_options": {
        **ENTRY,
        vol.Required("lot_id"): cv.string,
        vol.Required("occurred_on"): cv.string,
        vol.Optional("correcting"): cv.string,
    },
    "confirm_proposal": {
        **COMMAND,
        vol.Required("location_id"): cv.string,
        vol.Required("proposal_id"): cv.string,
    },
    "complete_task": {
        **COMMAND,
        vol.Required("task_id"): cv.string,
        vol.Required("task_revision"): vol.All(exact_int, vol.Range(min=0)),
        vol.Required("result"): dict,
    },
    "create_lot": {
        **COMMAND,
        vol.Required("code"): cv.string,
        vol.Required("quantity"): vol.All(exact_int, vol.Range(min=1)),
        vol.Required("started_on"): cv.string,
    },
    "create_movement": {
        **COMMAND,
        vol.Required("lot_id"): cv.string,
        vol.Required("occurred_on"): cv.string,
        vol.Required("source_location_id"): cv.string,
        vol.Required("destinations"): [dict],
        vol.Optional("mix_sources", default=False): cv.boolean,
        vol.Optional("notes", default=""): cv.string,
    },
}
MANUAL = {**COMMAND, vol.Required("location_id"): cv.string, vol.Required("reason"): cv.string}
MANUAL_RANGE = {
    vol.Required("limits"): dict,
    vol.Required("starts_at"): exact_int,
    vol.Required("ends_at"): exact_int,
}
SCHEMAS.update(
    {
        "schedule_manual": {**MANUAL, **MANUAL_RANGE},
        "replace_manual": {**MANUAL, **MANUAL_RANGE, vol.Required("manual_id"): cv.string},
        "finish_manual": {**MANUAL, vol.Required("manual_id"): cv.string},
        "correct_movement": {
            **SCHEMAS["create_movement"],
            vol.Required("movement_id"): cv.string,
            vol.Required("reason"): cv.string,
        },
    }
)


@callback
def async_register_services(hass):
    async def handle(call):
        entry = hass.config_entries.async_get_entry(call.data["config_entry_id"])
        if not entry or entry.domain != DOMAIN or entry.state is not ConfigEntryState.LOADED:
            raise ServiceValidationError("Select a loaded Registros Cámaras integration")
        coordinator = entry.runtime_data
        client = coordinator.client
        try:
            if call.service == "get_snapshot":
                await coordinator.async_refresh()
                if not coordinator.last_update_success:
                    raise HomeAssistantError("Snapshot unavailable")
                return coordinator.data.raw
            if call.service == "get_lots":
                params = {
                    key: call.data[key] for key in ("before", "search", "limit") if key in call.data
                }
                return await client.request(
                    "GET", f"production/{client.centre_id}/lots", params=params
                )
            if call.service == "get_movement_options":
                params = {"occurredOn": call.data["occurred_on"]}
                if "correcting" in call.data:
                    params["correcting"] = call.data["correcting"]
                return await client.request(
                    "GET",
                    f"production/{client.centre_id}/lots/{segment(call.data['lot_id'])}/movement-options",
                    params=params,
                )
            if call.service in {"get_lot", "get_movement_history"}:
                path = f"production/{client.centre_id}/lots/{segment(call.data['lot_id'])}"
                if call.service == "get_movement_history":
                    path += "/movements"
                return await client.request("GET", path)
            result = await client.command(call.service, dict(call.data))
        except AuthenticationError as err:
            entry.async_start_reauth(hass)
            raise HomeAssistantError("API key rejected") from err
        except (Conflict, Rejected) as err:
            raise ServiceValidationError(str(err)) from err
        except ApiError as err:
            raise HomeAssistantError(str(err)) from err
        # Once accepted, return the receipt even when the next poll fails.
        await coordinator.async_request_refresh()
        return result

    for name, schema in SCHEMAS.items():
        if not hass.services.has_service(DOMAIN, name):
            async_register_admin_service(
                hass,
                DOMAIN,
                name,
                handle,
                schema=vol.Schema(schema),
                supports_response=SupportsResponse.OPTIONAL,
            )
