"""Exercise the standalone cross-check harness, not a substitute for Worker tests."""

import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

from aiohttp import web
import pytest


@pytest.mark.parametrize("valid_ack", [True, False])
async def test_cross_script_real_http_and_native_store(aiohttp_client, socket_enabled, valid_ack):
    source = "https://disposable-source.example"
    new_source = "https://new-disposable-source.example"
    row = {
        "assignmentId": "closed",
        "sourceEventId": "1970-01-01T00:00:01+00:00:change:" + hashlib.sha256(b"20").hexdigest(),
        "observedAt": 1000,
        "mode": "live",
        "value": 20,
        "unit": "°C",
        "status": "valid",
    }
    rejected = {
        "apiVersion": 1,
        "commandId": "00000000-0000-4000-8000-000000000123",
        "centreId": "centre",
        "source": source,
        "readings": [row],
    }
    healthy = {
        **row,
        "assignmentId": "new-good",
        "sourceEventId": "1970-01-01T00:00:01.000001+00:00:change:"
        + hashlib.sha256(b"20").hexdigest(),
    }
    manifest = {
        "apiVersion": 1,
        "revision": 2,
        "maxBatchSize": 100,
        "maxBatchDelayMillis": 2000,
        "assignments": [
            {
                "id": "new-good",
                "source": new_source,
                "entityId": "sensor.temperature",
                "locationId": "chamber",
                "metric": "temperature",
                "position": 1,
                "startsAt": 0,
                "endsAt": None,
                "historyFrom": 0,
            }
        ],
    }
    attempts = []

    async def stream(request):
        assert request.headers["Authorization"] == "Bearer private-fixture-key"
        assert request.headers["X-Cross-Check"] == "forwarded"
        return web.json_response(manifest)

    async def push(request):
        assert request.headers["Authorization"] == "Bearer private-fixture-key"
        assert request.headers["X-Cross-Check"] == "forwarded"
        batch = await request.json()
        attempts.append(deepcopy(batch))
        if len(attempts) == 1:
            assert batch == rejected
            return web.json_response(
                {
                    "_tag": "SensorUpdatesRejected",
                    "apiVersion": 1,
                    "commandId": batch["commandId"],
                    "reason": "assignment_window",
                    "accepted": False,
                },
                status=422,
            )
        assert batch["readings"] == [healthy]
        assert batch["source"] == new_source
        assert batch["commandId"] != rejected["commandId"]
        command_id = batch["commandId"] if valid_ack else "wrong-command"
        return web.json_response(
            {
                "commandId": command_id,
                "stream": f"ReadingTransaction/{command_id}",
                "eventId": f"ReadingTransaction/{command_id}:4",
                "version": 4,
            }
        )

    app = web.Application()
    app.router.add_get("/api/environment/streams/centre", stream)
    app.router.add_post("/api/environment/updates", push)
    http = await aiohttp_client(app)
    config = {
        "url": str(http.make_url("/")),
        "headers": {
            "Authorization": "Bearer private-fixture-key",
            "X-Cross-Check": "forwarded",
        },
        "rejectedBatch": rejected,
        "healthySource": new_source,
        "healthyReading": healthy,
        "manifest": manifest,
    }
    script = Path(__file__).resolve().parents[1] / "scripts/check-updates-server.py"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(config).encode()), timeout=30
        )
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert b"private-fixture-key" not in stdout + stderr
    assert row["sourceEventId"].encode() not in stdout + stderr
    assert b"disposable-source.example" not in stdout + stderr
    assert len(attempts) == 2, (stdout, stderr)
    if valid_ack:
        assert process.returncode == 0, stderr.decode()
        assert stderr == b""
        assert json.loads(stdout) == {
            "ok": True,
            "storeVersion": 2,
            "rejectedAttempts": 1,
            "blockedBatches": 1,
            "blockedReports": 1,
            "healthyCommandId": attempts[1]["commandId"],
            "healthyReportsAcknowledged": 1,
            "blockedIntactAfterRestart": True,
            "ackDurableAfterRestart": True,
        }
    else:
        assert process.returncode == 1
        assert stdout == b""
        assert json.loads(stderr) == {"ok": False, "error": "AssertionError"}
