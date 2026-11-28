"""Native card-friendly chamber values, not a frontend credential proxy."""

from datetime import UTC, datetime

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import PERCENTAGE, UnitOfTemperature

from .entity import RegistrosEntity, discover

CAMERA_KEYS = (
    "quantity",
    "lots",
    "plan_status",
    "temperature_min",
    "temperature_max",
    "humidity_min",
    "humidity_max",
    "proposal",
    "setpoint_source",
    "manual_end",
)


async def async_setup_entry(hass, entry, async_add_entities):
    coordinator = entry.runtime_data
    async_add_entities([CentreSensor(coordinator, key) for key in ("pending_tasks", "due_tasks")])
    discover(
        coordinator,
        async_add_entities,
        lambda location: [CameraSensor(coordinator, key, location) for key in CAMERA_KEYS],
    )


class CameraSensor(RegistrosEntity, SensorEntity):
    def __init__(self, coordinator, key, location):
        super().__init__(coordinator, key, location)
        self.key = key
        if key.startswith("temperature_"):
            self._attr_device_class = SensorDeviceClass.TEMPERATURE
            self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        elif key.startswith("humidity_"):
            self._attr_device_class = SensorDeviceClass.HUMIDITY
            self._attr_native_unit_of_measurement = PERCENTAGE
        elif key == "plan_status":
            self._attr_device_class = SensorDeviceClass.ENUM
            self._attr_options = ["empty", "no_plan", "no_phase", "compatible", "conflict"]
        elif key == "manual_end":
            self._attr_device_class = SensorDeviceClass.TIMESTAMP
        elif key == "setpoint_source":
            self._attr_device_class = SensorDeviceClass.ENUM
            self._attr_options = ["manual", "automatic", "none"]

    @property
    def native_value(self):
        chamber = self.chamber
        if self.key == "quantity":
            return sum(o["quantity"] for o in chamber["occupants"])
        if self.key == "lots":
            return len(chamber["occupants"])
        if self.key == "plan_status":
            return chamber["status"]
        if self.key == "proposal":
            return "pending" if chamber["proposal"] else "none"
        if self.key == "setpoint_source":
            return (
                "manual" if chamber["manual"] else "automatic" if chamber["automatic"] else "none"
            )
        if self.key == "manual_end":
            return (
                datetime.fromtimestamp(chamber["manual"]["endsAt"] / 1000, UTC)
                if chamber["manual"]
                else None
            )
        metric, bound = self.key.split("_")
        return chamber["effective"][metric][bound] if chamber["effective"] else None

    @property
    def extra_state_attributes(self):
        attrs = super().extra_state_attributes
        if self.key == "lots":
            attrs["occupants"] = self.chamber["occupants"]
        if self.key == "proposal":
            attrs["proposal"] = self.chamber["proposal"]
        return attrs


class CentreSensor(RegistrosEntity, SensorEntity):
    def __init__(self, coordinator, key):
        super().__init__(coordinator, key)
        self.key = key

    @property
    def native_value(self):
        tasks = self.coordinator.data.tasks
        return (
            len(tasks) if self.key == "pending_tasks" else sum(t["status"] == "due" for t in tasks)
        )
