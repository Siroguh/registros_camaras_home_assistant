"""Run the actual Home Assistant integration lifecycle."""

from copy import deepcopy
from unittest.mock import AsyncMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.registros_camaras.api import decode_snapshot
from custom_components.registros_camaras.const import DEFAULT_CENTRE, DOMAIN

pytest_plugins = "pytest_homeassistant_custom_component"

LIMITS = {"temperature": {"min": 2, "max": 6}, "humidity": {"min": 50, "max": 70}}
RAW = {
    "apiVersion": 1,
    "centreId": DEFAULT_CENTRE,
    "revision": 7,
    "planRevision": 7,
    "sampledAt": "2026-11-27T12:00:00Z",
    "status": "ready",
    "lotCodes": {"00000000-0000-4000-8000-000000000101": "A"},
    "locations": [
        {
            "id": "cam-1",
            "centreId": DEFAULT_CENTRE,
            "name": "Cámara 1",
            "kind": "chamber",
            "archived": False,
            "description": None,
        }
    ],
    "chambers": [
        {
            "locationId": "cam-1",
            "status": "compatible",
            "ideal": LIMITS,
            "automatic": LIMITS,
            "automaticId": "automatic:6",
            "automaticDeviations": None,
            "automaticOrigin": {
                "startsAt": 1795777100000,
                "replacesId": None,
                "actor": {"type": "user", "userId": "operator"},
                "confirmationCommandId": "00000000-0000-4000-8000-000000000122",
            },
            "effective": LIMITS,
            "manual": None,
            "proposal": {
                "id": "proposal:7",
                "contextKey": "context:7",
                "limits": LIMITS,
                "deviations": None,
                "createdAt": 1795777200000,
                "dueAt": 1795777500000,
                "overdue": True,
            },
            "occupants": [
                {
                    "lotId": "00000000-0000-4000-8000-000000000101",
                    "quantity": 45,
                    "age": 3,
                    "baseVersionId": None,
                    "privateRevisionId": None,
                    "stepKey": None,
                    "limits": LIMITS,
                }
            ],
        }
    ],
    "tasks": [
        {
            "id": "task:1",
            "lotId": "00000000-0000-4000-8000-000000000101",
            "assignmentId": "00000000-0000-4000-8000-000000000102",
            "versionId": "00000000-0000-4000-8000-000000000103",
            "privateRevisionId": "00000000-0000-4000-8000-000000000104",
            "revision": 3,
            "dueAt": 1795777200000,
            "createdAt": 1795777100000,
            "dueRecordedAt": 1795777200000,
            "completedAt": None,
            "completedBy": None,
            "result": None,
            "cancelledAt": None,
            "cancellationReason": None,
            "replacementId": None,
            "status": "due",
            "step": {
                "kind": "curing_review",
                "key": "review",
                "title": "Revisión",
                "instructions": "Revisar",
                "day": 1,
            },
        }
    ],
}


@pytest.fixture(autouse=True)
def custom_integrations(enable_custom_integrations):
    yield


@pytest.fixture
def stream_manifest():
    with patch(
        "custom_components.registros_camaras.api.RegistrosClient.sensor_stream",
        new=AsyncMock(
            return_value={
                "apiVersion": 1,
                "revision": 0,
                "assignments": [],
                "maxBatchSize": 100,
                "maxBatchDelayMillis": 2000,
            }
        ),
    ) as manifest:
        yield manifest


@pytest.fixture
def raw():
    return deepcopy(RAW)


@pytest.fixture
def receipt():
    return {
        "commandId": "00000000-0000-4000-8000-000000000123",
        "stream": f"ProductionCentre/{DEFAULT_CENTRE}",
        "eventId": "production-fact:confirmed",
        "version": 8,
    }


@pytest.fixture
def client(raw, stream_manifest):
    with patch(
        "custom_components.registros_camaras.api.RegistrosClient.snapshot",
        new=AsyncMock(return_value=decode_snapshot(raw, DEFAULT_CENTRE)),
    ) as snapshot:
        yield snapshot


@pytest.fixture
def entry(hass):
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=2,
        title="Registros Test",
        unique_id="server-id",
        data={
            "url": "https://registros.example",
            "api_key": "test-key",
            "centre_id": DEFAULT_CENTRE,
        },
    )
    entry.add_to_hass(hass)
    return entry
