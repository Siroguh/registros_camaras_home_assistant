"""UI configuration, reauthentication and reconfiguration."""

from collections.abc import Mapping
from hashlib import sha256
from typing import Any

import probatio as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_API_KEY, CONF_URL
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import TextSelector, TextSelectorConfig, TextSelectorType

from .api import ApiError, AuthenticationError, IncompatibleApi, RegistrosClient, normalize_url
from .const import CONF_CENTRE_ID, DEFAULT_CENTRE, DOMAIN


class RegistrosConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 2

    async def async_step_user(self, user_input: dict | None = None) -> ConfigFlowResult:
        return await self._configure("user", user_input)

    async def async_step_reauth(self, entry_data: Mapping[str, Any]) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict | None = None) -> ConfigFlowResult:
        return await self._configure("reauth_confirm", user_input, self._get_reauth_entry())

    async def async_step_reconfigure(self, user_input: dict | None = None) -> ConfigFlowResult:
        return await self._configure("reconfigure", user_input, self._get_reconfigure_entry())

    async def _configure(
        self, step: str, user_input: dict | None, entry: ConfigEntry | None = None
    ):
        errors = {}
        if user_input is not None:
            data = {
                CONF_URL: entry.data[CONF_URL]
                if step == "reauth_confirm"
                else user_input[CONF_URL],
                CONF_CENTRE_ID: entry.data[CONF_CENTRE_ID] if entry else user_input[CONF_CENTRE_ID],
                CONF_API_KEY: user_input.get(CONF_API_KEY, ""),
            }
            if entry and step == "reconfigure" and not data[CONF_API_KEY].strip():
                data[CONF_API_KEY] = entry.data[CONF_API_KEY]
            try:
                data[CONF_URL] = normalize_url(data[CONF_URL])
                data[CONF_API_KEY] = data[CONF_API_KEY].strip()
                if not data[CONF_API_KEY]:
                    raise AuthenticationError("Empty key")
                client = RegistrosClient(
                    async_get_clientsession(self.hass),
                    data[CONF_URL],
                    data[CONF_API_KEY],
                    data[CONF_CENTRE_ID],
                )
                await client.snapshot()
                await client.sensor_stream()
            except ValueError:
                errors["base"] = "invalid_address"
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except IncompatibleApi:
                errors["base"] = "incompatible_api"
            except ApiError:
                errors["base"] = "cannot_connect"
            else:
                identity = sha256(f"{data[CONF_URL]}|{data[CONF_CENTRE_ID]}".encode()).hexdigest()
                for other in self._async_current_entries():
                    if other.entry_id != (entry.entry_id if entry else None) and (
                        other.data.get(CONF_URL) == data[CONF_URL]
                        and other.data.get(CONF_CENTRE_ID) == data[CONF_CENTRE_ID]
                    ):
                        return self.async_abort(reason="already_configured")
                if entry:
                    # Preserve registry IDs when the same installation changes its URL/key.
                    return self.async_update_reload_and_abort(entry, data=data, unique_id=identity)
                await self.async_set_unique_id(identity)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Registros Cámaras · {data[CONF_CENTRE_ID]}", data=data
                )
        fields = {}
        if step != "reauth_confirm":
            fields[vol.Required(CONF_URL)] = TextSelector(
                TextSelectorConfig(type=TextSelectorType.URL)
            )
        if not entry:
            fields[vol.Required(CONF_CENTRE_ID, default=DEFAULT_CENTRE)] = str
        marker = vol.Optional if step == "reconfigure" else vol.Required
        fields[marker(CONF_API_KEY)] = TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        )
        schema = vol.Schema(fields)
        if entry and step == "reconfigure":
            schema = self.add_suggested_values_to_schema(schema, {CONF_URL: entry.data[CONF_URL]})
        return self.async_show_form(step_id=step, data_schema=schema, errors=errors)
