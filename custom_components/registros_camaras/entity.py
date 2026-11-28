"""Stable devices/entities and discovery of newly added chambers."""

from collections.abc import Callable

from homeassistant.core import callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import RegistrosCoordinator


class RegistrosEntity(CoordinatorEntity[RegistrosCoordinator]):
    _attr_has_entity_name = True

    def __init__(self, coordinator: RegistrosCoordinator, key: str, location_id: str | None = None):
        super().__init__(coordinator)
        self.location_id = location_id
        entry_id = coordinator.config_entry.entry_id
        self._attr_unique_id = f"{entry_id}_{location_id or 'centre'}_{key}"
        self._attr_translation_key = key
        root = (DOMAIN, entry_id)
        if location_id:
            location = coordinator.data.locations[location_id]
            self._attr_device_info = DeviceInfo(
                identifiers={(DOMAIN, f"{entry_id}_{location_id}")},
                name=location["name"],
                via_device_id=coordinator.root_device_id,
                manufacturer="Registros Cámaras",
                model="Cámara",
            )
        else:
            self._attr_device_info = DeviceInfo(
                identifiers={root},
                name=coordinator.config_entry.title,
                manufacturer="Registros Cámaras",
                model="Centro de producción",
                entry_type=DeviceEntryType.SERVICE,
            )

    @property
    def available(self) -> bool:
        return (
            super().available
            and self.coordinator.data.ready
            and (
                self.location_id is None
                or (
                    self.location_id in self.coordinator.data.locations
                    and not self.coordinator.data.locations[self.location_id]["archived"]
                )
            )
        )

    @property
    def chamber(self) -> dict:
        return self.coordinator.data.chambers.get(
            self.location_id,
            {
                "status": "empty",
                "occupants": [],
                "effective": None,
                "ideal": None,
                "automatic": None,
                "automaticId": None,
                "automaticOrigin": None,
                "automaticDeviations": None,
                "manual": None,
                "proposal": None,
            },
        )

    @property
    def extra_state_attributes(self) -> dict:
        return {
            "active_lots": len(self.chamber["occupants"]) if self.location_id else None,
            "centre_id": self.coordinator.client.centre_id,
            "location_id": self.location_id,
            "revision": self.coordinator.data.revision,
        }


@callback
def discover(coordinator: RegistrosCoordinator, add_entities: Callable, factory: Callable) -> None:
    known: set[str] = set()

    @callback
    def update() -> None:
        new = [
            key
            for key, loc in coordinator.data.locations.items()
            if key not in known and not loc["archived"]
        ]
        known.update(new)
        add_entities([entity for key in new for entity in factory(key)])

    update()
    coordinator.config_entry.async_on_unload(coordinator.async_add_listener(update))
