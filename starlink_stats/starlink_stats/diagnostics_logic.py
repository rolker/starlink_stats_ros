# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
Pure-Python diagnostic logic for the Starlink node.

This module avoids heavy ROS 2 runtime dependencies (``rclpy``) and gRPC
imports so it can be unit-tested against hand-built status dicts. It still
imports ``diagnostic_msgs.msg.DiagnosticStatus`` for the standard diagnostic
level constants (OK / WARN / ERROR / STALE).
"""

from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from diagnostic_msgs.msg import DiagnosticStatus


# --- Mapping tables ---

# Maps `status.state` enum string to a baseline diagnostic level.
# `SEARCHING` is OK initially; the link task escalates to WARN once the dish
# has been searching longer than `searching_warn_delay_sec`.
STATE_LEVEL_MAP: dict[str, int] = {
    'UNKNOWN': DiagnosticStatus.WARN,
    'CONNECTED': DiagnosticStatus.OK,
    'BOOTING': DiagnosticStatus.OK,
    'SEARCHING': DiagnosticStatus.OK,
    'STOWED': DiagnosticStatus.WARN,
    'THERMAL_SHUTDOWN': DiagnosticStatus.ERROR,
    'SLEEPING': DiagnosticStatus.OK,
    'NO_SATS': DiagnosticStatus.WARN,
    'OBSTRUCTED': DiagnosticStatus.WARN,
    'NETWORK_ISSUE': DiagnosticStatus.WARN,
}

# Alerts that are specifically about thermal condition. Handled by the
# `thermal` task; the generic `alerts` task skips them to avoid double-reporting.
THERMAL_ALERTS: frozenset[str] = frozenset({
    'thermal_throttle',
    'thermal_shutdown',
    'is_heating',
    'power_supply_thermal_throttle',
})

# Per-alert level table. Unknown alerts default to WARN in the task functions.
ALERT_LEVEL_MAP: dict[str, int] = {
    # Thermal
    'thermal_shutdown': DiagnosticStatus.ERROR,
    'thermal_throttle': DiagnosticStatus.WARN,
    'power_supply_thermal_throttle': DiagnosticStatus.WARN,
    'is_heating': DiagnosticStatus.OK,  # informational
    # Mechanical / installation
    'motors_stuck': DiagnosticStatus.ERROR,
    'mast_not_near_vertical': DiagnosticStatus.WARN,
    'install_pending': DiagnosticStatus.OK,
    # Location / mobility
    'unexpected_location': DiagnosticStatus.WARN,
    'moving_while_not_mobile': DiagnosticStatus.WARN,
    'moving_fast_while_not_aviation': DiagnosticStatus.WARN,
    # Link / network
    'slow_ethernet_speeds': DiagnosticStatus.WARN,
    'roaming': DiagnosticStatus.OK,  # informational
    'dbf_telem_stale': DiagnosticStatus.WARN,
    'low_motor_current': DiagnosticStatus.WARN,
    'lower_signal_than_predicted': DiagnosticStatus.WARN,
    # Other
    'is_power_save_idle': DiagnosticStatus.OK,
    'obstruction_map_reset': DiagnosticStatus.OK,
}

# Curated allowlist of dotted paths to include as KeyValue pairs per task.
# When `dump_all_fields` is true the node appends a full flattened dump.
CURATED_KEYS: dict[str, list[str]] = {
    'comms': [
        'device_info.id',
        'device_info.hardware_version',
        'device_info.software_version',
        'device_info.country_code',
    ],
    'state': [
        'state',
        'device_state.uptime_s',
        'seconds_to_first_nonempty_slot',
    ],
    'link': [
        'pop_ping_drop_rate',
        'pop_ping_latency_ms',
        'downlink_throughput_bps',
        'uplink_throughput_bps',
        'snr_above_noise_floor',
        'is_snr_above_noise_floor',
        'is_snr_persistently_low',
        'eth_speed_mbps',
    ],
    'obstruction': [
        'obstruction_stats.fraction_obstructed',
        'obstruction_stats.currently_obstructed',
        'obstruction_stats.time_obstructed',
        'obstruction_stats.valid_s',
        'obstruction_stats.avg_prolonged_obstruction_duration_s',
    ],
    'thermal': [
        'alerts.thermal_throttle',
        'alerts.thermal_shutdown',
        'alerts.is_heating',
        'alerts.power_supply_thermal_throttle',
    ],
    'alerts': [],  # populated dynamically from active alerts
}


@dataclass
class Thresholds:
    """Operator-tunable thresholds for numeric diagnostics."""

    obstruction_warn_fraction: float = 0.005
    obstruction_error_fraction: float = 0.05
    ping_drop_warn_rate: float = 0.01
    ping_drop_error_rate: float = 0.10
    ping_latency_warn_ms: float = 100.0
    ping_latency_error_ms: float = 500.0
    snr_warn_db: float = 9.0
    searching_warn_delay_sec: float = 60.0
    stale_timeout_sec: float = 5.0


# --- Helpers ---

def get_dotted(d: Optional[dict], path: str, default: Any = None) -> Any:
    """Return ``d[a][b][c]`` for a dotted ``path``; ``default`` on any miss."""
    if not isinstance(d, dict):
        return default
    cur: Any = d
    for key in path.split('.'):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return default
    return cur


def flatten(d: MutableMapping, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flatten nested dicts/lists into a single-level dict.

    Used only for the ``dump_all_fields`` debug escape hatch.
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                indexed_key = f'{new_key}_{i}'
                if isinstance(item, MutableMapping):
                    items.extend(flatten(item, indexed_key, sep=sep).items())
                else:
                    items.append((indexed_key, item))
        else:
            items.append((new_key, v))
    return dict(items)


# --- Diagnostic task logic ---

def diagnose_state(
    status: dict,
    known_states: Iterable[str],
    unknown_seen: set,
) -> tuple[int, str, list[str]]:
    """
    Diagnose the dish's reported state enum.

    Returns ``(level, message, newly_unknown_states)``. ``newly_unknown_states``
    is the list of state names encountered for the first time that the caller
    should log and add to ``unknown_seen``.
    """
    state = status.get('state')
    if state is None:
        return DiagnosticStatus.WARN, 'state field missing from response', []
    state_str = str(state)
    known = set(known_states)
    if state_str not in known:
        new = [] if state_str in unknown_seen else [state_str]
        return DiagnosticStatus.WARN, f'unknown dish state: {state_str}', new
    level = STATE_LEVEL_MAP.get(state_str, DiagnosticStatus.WARN)
    return level, f'state={state_str}', []


def diagnose_link(
    status: dict,
    thresholds: Thresholds,
    first_searching_monotonic: Optional[float],
    now_monotonic: float,
) -> tuple[int, str]:
    """Diagnose link quality: state, ping drop/latency, SNR."""
    summary_bits: list[str] = []
    level = DiagnosticStatus.OK

    state = status.get('state')
    if state is not None:
        summary_bits.append(str(state))
        level = max(level, STATE_LEVEL_MAP.get(str(state), DiagnosticStatus.WARN))

    # SEARCHING grace window
    if (
        state == 'SEARCHING'
        and first_searching_monotonic is not None
        and (now_monotonic - first_searching_monotonic) > thresholds.searching_warn_delay_sec
    ):
        elapsed = now_monotonic - first_searching_monotonic
        level = max(level, DiagnosticStatus.WARN)
        summary_bits.append(f'searching {elapsed:.0f}s')

    drop = status.get('pop_ping_drop_rate')
    if isinstance(drop, (int, float)):
        summary_bits.append(f'{100.0 * drop:.1f}% drop')
        if drop >= thresholds.ping_drop_error_rate:
            level = max(level, DiagnosticStatus.ERROR)
        elif drop >= thresholds.ping_drop_warn_rate:
            level = max(level, DiagnosticStatus.WARN)

    lat = status.get('pop_ping_latency_ms')
    if isinstance(lat, (int, float)):
        summary_bits.append(f'{lat:.0f}ms')
        if lat >= thresholds.ping_latency_error_ms:
            level = max(level, DiagnosticStatus.ERROR)
        elif lat >= thresholds.ping_latency_warn_ms:
            level = max(level, DiagnosticStatus.WARN)

    snr = status.get('snr_above_noise_floor')
    if isinstance(snr, (int, float)):
        summary_bits.append(f'{snr:.1f}dB SNR')
        if snr < thresholds.snr_warn_db:
            level = max(level, DiagnosticStatus.WARN)
    elif status.get('is_snr_above_noise_floor') is False:
        level = max(level, DiagnosticStatus.WARN)
        summary_bits.append('SNR below floor')

    return level, ', '.join(summary_bits) if summary_bits else 'no link data'


def diagnose_obstruction(status: dict, thresholds: Thresholds) -> tuple[int, str]:
    """Diagnose obstruction percentage."""
    frac = get_dotted(status, 'obstruction_stats.fraction_obstructed')
    currently = bool(get_dotted(status, 'obstruction_stats.currently_obstructed', False))
    if not isinstance(frac, (int, float)):
        return DiagnosticStatus.OK, 'obstruction data unavailable'

    level = DiagnosticStatus.OK
    if frac >= thresholds.obstruction_error_fraction:
        level = DiagnosticStatus.ERROR
    elif frac >= thresholds.obstruction_warn_fraction:
        level = DiagnosticStatus.WARN
    if currently:
        level = max(level, DiagnosticStatus.WARN)

    msg = f'{100.0 * frac:.2f}% obstructed'
    if currently:
        msg += ' (currently)'
    return level, msg


def diagnose_thermal(status: dict) -> tuple[int, str]:
    """Diagnose thermal alerts only."""
    alerts = status.get('alerts') or {}
    if not isinstance(alerts, dict):
        return DiagnosticStatus.OK, 'no thermal alerts'
    active = [a for a in THERMAL_ALERTS if alerts.get(a)]
    level = DiagnosticStatus.OK
    for a in active:
        level = max(level, ALERT_LEVEL_MAP.get(a, DiagnosticStatus.WARN))
    if active:
        return level, 'active: ' + ', '.join(sorted(active))
    return level, 'no thermal alerts'


def diagnose_alerts(
    status: dict,
    known_alerts: Iterable[str],
    unknown_seen: set,
) -> tuple[int, str, list[str], list[str]]:
    """
    Diagnose non-thermal alerts.

    Returns ``(level, message, active_alerts, newly_unknown_alerts)``.
    """
    alerts = status.get('alerts') or {}
    if not isinstance(alerts, dict):
        return DiagnosticStatus.OK, 'no alerts', [], []

    known = set(known_alerts)
    level = DiagnosticStatus.OK
    active: list[str] = []
    new_unknown: list[str] = []
    for name, val in alerts.items():
        if name in THERMAL_ALERTS:  # handled by thermal task
            continue
        if not val:
            continue
        active.append(name)
        if name in known:
            level = max(level, ALERT_LEVEL_MAP.get(name, DiagnosticStatus.WARN))
        else:
            level = max(level, DiagnosticStatus.WARN)
            if name not in unknown_seen:
                new_unknown.append(name)

    if active:
        return level, 'active: ' + ', '.join(sorted(active)), sorted(active), new_unknown
    return level, 'no alerts', [], new_unknown
