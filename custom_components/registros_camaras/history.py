"""Bounded Recorder reads using HA's own query, session and state conversion.

HA 2026.10.3's public get_significant_states has no row limit; the alternative
state_changes_during_period(limit=...) drops attribute-only reports. Isolate the
native history builders here, adding only a row cap and deterministic tie order.
The native metadata_id/last_updated_ts range query seeks retained rows directly;
an exhausted page proves the rest of the requested interval empty. Recovery can
therefore start at historyFrom=0 without iterating empty time windows. Each pass
materializes at most PAGE_REPORTS + 1 rows, independent of the interval's age.
There is no database connection, SQL text, or schema maintained by this adapter.
Keep real-Recorder compatibility tests when updating the minimum HA version.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
import math

from homeassistant.const import (
    COMPRESSED_STATE_ATTRIBUTES,
    COMPRESSED_STATE_LAST_UPDATED,
    COMPRESSED_STATE_STATE,
)
from homeassistant.core import HomeAssistant, State
from homeassistant.exceptions import HomeAssistantError

PAGE_REPORTS = 1000


class HistoryWindowFull(HomeAssistantError):
    """One millisecond exceeds the bounded native page; its cursor must not move."""


@dataclass(frozen=True)
class HistoryPage:
    states: list[State]
    end: int


def recorder_boundary(millis: int) -> float:
    """First raw float in the live capture millisecond bucket (datetime rounding).

    Recorder stores floats, while live identities/observedAt use datetime's rounded
    microseconds. Invert that same monotone conversion, rather than assuming that
    millis / 1000 is the boundary. Adjacent float bounds keep SQL pages disjoint.
    """
    low, high = millis / 1000 - 0.002, millis / 1000 + 0.002
    while math.nextafter(low, math.inf) < high:
        middle = low + (high - low) / 2
        if int(datetime.fromtimestamp(middle, UTC).timestamp() * 1000) < millis:
            low = middle
        else:
            high = middle
    return high


def read_page(hass: HomeAssistant, entity_id: str, start: int, end: int) -> HistoryPage:
    """Read bounded actual history, including empty gaps, without splitting a millisecond."""
    from homeassistant.components.recorder import get_instance
    from homeassistant.components.recorder.db_schema import States
    from homeassistant.components.recorder.history import (
        _generate_significant_states_with_session_stmt,
        _row_field_indices,
        _sorted_states_to_dict,
    )
    from homeassistant.components.recorder.util import execute_stmt_lambda_element, session_scope

    recorder = get_instance(hass)
    with session_scope(hass=hass, read_only=True) as session:
        metadata_id = recorder.states_meta_manager.get(entity_id, session, False)
        if metadata_id is None:
            return HistoryPage([], end)
        stmt = _generate_significant_states_with_session_stmt(
            math.nextafter(recorder_boundary(start), -math.inf),
            recorder_boundary(end),
            metadata_id,
            [metadata_id],
            [],
            False,  # Include attribute-only changes and forced reports.
            False,  # Preserve units.
            False,  # No synthetic start-of-window state.
            None,
            recorder.database_engine.optimizer.slow_dependent_subquery,
        )
        limit = PAGE_REPORTS + 1
        stmt += lambda query: query.order_by(States.state_id).limit(limit)
        rows = execute_stmt_lambda_element(
            session, stmt, None, datetime.fromtimestamp(end / 1000, UTC), orm_rows=False
        )
        groups = _sorted_states_to_dict(
            rows,
            None,
            [entity_id],
            {entity_id: metadata_id},
            minimal_response=False,
            compressed_state_format=True,
            no_attributes=False,
            field_indices=_row_field_indices(False, True, False),
        )
    raw = groups.get(entity_id, [])
    if len(raw) > PAGE_REPORTS:
        last_at = datetime.fromtimestamp(raw[-1][COMPRESSED_STATE_LAST_UPDATED], UTC)
        end = min(end, int(last_at.timestamp() * 1000))
        if end <= start:
            raise HistoryWindowFull("Coincident Recorder group exceeds page capacity")
    states = []
    for row in raw:
        # Unlike LazyState, compressed conversion preserves NULL tombstones vs
        # a genuine empty string. unknown/unavailable/empty strings are reports.
        if row[COMPRESSED_STATE_STATE] is None:
            continue
        at = datetime.fromtimestamp(row[COMPRESSED_STATE_LAST_UPDATED], UTC)
        if start <= int(at.timestamp() * 1000) < end:
            states.append(
                State(
                    entity_id,
                    row[COMPRESSED_STATE_STATE],
                    row[COMPRESSED_STATE_ATTRIBUTES],
                    last_updated=at,
                )
            )
    return HistoryPage(states, end)
