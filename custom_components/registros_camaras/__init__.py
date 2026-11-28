"""Registros Cámaras 2 integration, based on custom-components/blueprint."""

import logging

from homeassistant.const import CONF_API_KEY, CONF_URL, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import ApiError, AuthenticationError, RegistrosClient, Snapshot
from .const import CONF_CENTRE_ID, DOMAIN
from .coordinator import RegistrosConfigEntry, RegistrosCoordinator
from .services import async_register_services

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)
PLATFORMS = [Platform.SENSOR, Platform.BINARY_SENSOR, Platform.BUTTON]
_LOGGER = logging.getLogger(__name__)


async def cleanup_failed_setup(coordinator: RegistrosCoordinator) -> None:
    """Attempt the full flush without replacing setup's retry/authentication outcome."""
    try:
        await coordinator.updates.stop()
    except (HomeAssistantError, OSError) as err:
        _LOGGER.warning("Setup cleanup persistence pending (%s)", type(err).__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    async_register_services(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: RegistrosConfigEntry) -> bool:
    client = RegistrosClient(
        async_get_clientsession(hass),
        entry.data[CONF_URL],
        entry.data[CONF_API_KEY],
        entry.data[CONF_CENTRE_ID],
    )
    coordinator = RegistrosCoordinator(hass, entry, client)
    try:
        await coordinator.updates.prepare()
        try:
            await coordinator.async_config_entry_first_refresh()
        except ConfigEntryNotReady:
            cached = coordinator.updates.spool.data
            if "snapshot" not in cached or "manifest" not in cached:
                raise
            # A real last-known projection remains unavailable until polling succeeds.
            coordinator.data = Snapshot(cached["snapshot"])
        else:
            await coordinator.updates.flush_snapshot()
        await coordinator.updates.start()
    except ConfigEntryAuthFailed:
        await cleanup_failed_setup(coordinator)
        raise
    except AuthenticationError as err:
        await cleanup_failed_setup(coordinator)
        raise ConfigEntryAuthFailed("API key rejected") from err
    except (ApiError, OSError, HomeAssistantError) as err:
        await cleanup_failed_setup(coordinator)
        raise ConfigEntryNotReady("Sensor update transport unavailable") from err
    except BaseException:
        await cleanup_failed_setup(coordinator)
        raise
    entry.runtime_data = coordinator
    try:
        root = dr.async_get(hass).async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.title,
            manufacturer="Registros Cámaras",
            model="Centro de producción",
            entry_type=dr.DeviceEntryType.SERVICE,
        )
        coordinator.root_device_id = root.id
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        await cleanup_failed_setup(coordinator)
        raise
    return True


async def async_unload_entry(hass: HomeAssistant, entry: RegistrosConfigEntry) -> bool:
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False
    await entry.runtime_data.updates.stop()
    return True
