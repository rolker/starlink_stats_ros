# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
Unit tests for the pure-Python diagnostic logic.

These tests hand-build status dicts rather than exercising the gRPC client
so they run without the generated ``spacex_api`` protobuf modules.
"""

from diagnostic_msgs.msg import DiagnosticStatus
import pytest

from starlink_stats.diagnostics_logic import (
    ALERT_LEVEL_MAP,
    diagnose_alerts,
    diagnose_link,
    diagnose_obstruction,
    diagnose_state,
    diagnose_thermal,
    flatten,
    get_dotted,
    STATE_LEVEL_MAP,
    THERMAL_ALERTS,
    Thresholds,
)


OK = DiagnosticStatus.OK
WARN = DiagnosticStatus.WARN
ERROR = DiagnosticStatus.ERROR


# --- helpers ---

def test_get_dotted_happy_path():
    d = {'a': {'b': {'c': 42}}}
    assert get_dotted(d, 'a.b.c') == 42


def test_get_dotted_missing_returns_default():
    d = {'a': {}}
    assert get_dotted(d, 'a.b.c') is None
    assert get_dotted(d, 'a.b.c', default='x') == 'x'


def test_get_dotted_non_dict_input():
    assert get_dotted(None, 'a.b') is None
    assert get_dotted([], 'a') is None


def test_flatten_nested():
    flat = flatten({'a': {'b': 1, 'c': {'d': 2}}, 'e': 3})
    assert flat == {'a_b': 1, 'a_c_d': 2, 'e': 3}


def test_flatten_list_of_scalars():
    flat = flatten({'xs': [10, 20]})
    assert flat == {'xs_0': 10, 'xs_1': 20}


def test_flatten_list_of_dicts():
    flat = flatten({'items': [{'v': 1}, {'v': 2}]})
    assert flat == {'items_0_v': 1, 'items_1_v': 2}


# --- diagnose_state ---

@pytest.mark.parametrize('state,expected', [
    ('CONNECTED', OK),
    ('BOOTING', OK),
    ('SEARCHING', OK),  # escalation handled by link task
    ('STOWED', WARN),
    ('THERMAL_SHUTDOWN', ERROR),
    ('UNKNOWN', WARN),
])
def test_diagnose_state_known_values(state, expected):
    level, msg, new_unknown = diagnose_state(
        {'state': state}, STATE_LEVEL_MAP.keys(), set(),
    )
    assert level == expected
    assert state in msg
    assert new_unknown == []


def test_diagnose_state_missing_field():
    """Missing state field is OK — normal for some dish variants/firmware."""
    level, msg, new_unknown = diagnose_state({}, STATE_LEVEL_MAP.keys(), set())
    assert level == OK
    assert 'not reported' in msg
    assert new_unknown == []


def test_diagnose_state_unknown_reports_once():
    seen = set()
    level1, msg1, new1 = diagnose_state(
        {'state': 'NEW_FIRMWARE_STATE'}, STATE_LEVEL_MAP.keys(), seen,
    )
    assert level1 == WARN
    assert 'NEW_FIRMWARE_STATE' in msg1
    assert new1 == ['NEW_FIRMWARE_STATE']

    # Simulate caller adding to seen set
    seen.update(new1)

    level2, _, new2 = diagnose_state(
        {'state': 'NEW_FIRMWARE_STATE'}, STATE_LEVEL_MAP.keys(), seen,
    )
    assert level2 == WARN
    assert new2 == []  # already reported


# --- diagnose_link ---

def test_diagnose_link_connected_clean():
    level, msg = diagnose_link(
        {
            'state': 'CONNECTED',
            'pop_ping_drop_rate': 0.0,
            'pop_ping_latency_ms': 35.0,
            'snr_above_noise_floor': 12.5,
        },
        Thresholds(),
        first_searching_monotonic=None,
        now_monotonic=100.0,
    )
    assert level == OK
    assert 'CONNECTED' in msg
    assert '35' in msg


def test_diagnose_link_ping_drop_warn():
    level, _ = diagnose_link(
        {'state': 'CONNECTED', 'pop_ping_drop_rate': 0.05},
        Thresholds(),
        None,
        100.0,
    )
    assert level == WARN


def test_diagnose_link_ping_drop_error():
    level, _ = diagnose_link(
        {'state': 'CONNECTED', 'pop_ping_drop_rate': 0.20},
        Thresholds(),
        None,
        100.0,
    )
    assert level == ERROR


def test_diagnose_link_latency_error():
    level, _ = diagnose_link(
        {'state': 'CONNECTED', 'pop_ping_latency_ms': 1000.0},
        Thresholds(),
        None,
        100.0,
    )
    assert level == ERROR


def test_diagnose_link_low_snr_warn():
    level, msg = diagnose_link(
        {'state': 'CONNECTED', 'snr_above_noise_floor': 3.0},
        Thresholds(snr_warn_db=9.0),
        None,
        100.0,
    )
    assert level == WARN
    assert 'SNR' in msg or 'dB' in msg


def test_diagnose_link_max_level_wins():
    # Low SNR (WARN) + high drop (ERROR) -> ERROR
    level, _ = diagnose_link(
        {
            'state': 'CONNECTED',
            'snr_above_noise_floor': 3.0,
            'pop_ping_drop_rate': 0.5,
        },
        Thresholds(),
        None,
        100.0,
    )
    assert level == ERROR


def test_diagnose_link_searching_within_grace():
    level, msg = diagnose_link(
        {'state': 'SEARCHING'},
        Thresholds(searching_warn_delay_sec=60.0),
        first_searching_monotonic=100.0,
        now_monotonic=120.0,  # only 20s in
    )
    assert level == OK
    assert 'SEARCHING' in msg


def test_diagnose_link_searching_past_grace():
    level, msg = diagnose_link(
        {'state': 'SEARCHING'},
        Thresholds(searching_warn_delay_sec=60.0),
        first_searching_monotonic=100.0,
        now_monotonic=200.0,  # 100s in
    )
    assert level == WARN
    assert 'searching' in msg.lower()


def test_diagnose_link_is_snr_below_floor_without_value():
    level, _ = diagnose_link(
        {'state': 'CONNECTED', 'is_snr_above_noise_floor': False},
        Thresholds(),
        None,
        100.0,
    )
    assert level == WARN


def test_diagnose_link_stowed_state_warn():
    level, _ = diagnose_link(
        {'state': 'STOWED'},
        Thresholds(),
        None,
        100.0,
    )
    assert level == WARN


# --- diagnose_obstruction ---

def test_diagnose_obstruction_clean():
    level, msg = diagnose_obstruction(
        {'obstruction_stats': {'fraction_obstructed': 0.001}},
        Thresholds(obstruction_warn_fraction=0.005, obstruction_error_fraction=0.05),
    )
    assert level == OK
    assert '%' in msg


def test_diagnose_obstruction_warn_band():
    level, _ = diagnose_obstruction(
        {'obstruction_stats': {'fraction_obstructed': 0.02}},
        Thresholds(obstruction_warn_fraction=0.005, obstruction_error_fraction=0.05),
    )
    assert level == WARN


def test_diagnose_obstruction_error_band():
    level, _ = diagnose_obstruction(
        {'obstruction_stats': {'fraction_obstructed': 0.10}},
        Thresholds(obstruction_warn_fraction=0.005, obstruction_error_fraction=0.05),
    )
    assert level == ERROR


def test_diagnose_obstruction_currently_flag_escalates():
    level, msg = diagnose_obstruction(
        {'obstruction_stats': {
            'fraction_obstructed': 0.0,
            'currently_obstructed': True,
        }},
        Thresholds(),
    )
    assert level == WARN
    assert 'currently' in msg


def test_diagnose_obstruction_missing_data():
    level, msg = diagnose_obstruction({}, Thresholds())
    assert level == OK
    assert 'unavailable' in msg


# --- diagnose_thermal ---

def test_diagnose_thermal_no_alerts():
    level, msg = diagnose_thermal({'alerts': {}})
    assert level == OK
    assert 'no thermal' in msg


def test_diagnose_thermal_shutdown_is_error():
    level, msg = diagnose_thermal({'alerts': {'thermal_shutdown': True}})
    assert level == ERROR
    assert 'thermal_shutdown' in msg


def test_diagnose_thermal_throttle_is_warn():
    level, _ = diagnose_thermal({'alerts': {'thermal_throttle': True}})
    assert level == WARN


def test_diagnose_thermal_is_heating_informational():
    # is_heating alone shouldn't elevate beyond OK in current table.
    level, _ = diagnose_thermal({'alerts': {'is_heating': True}})
    assert level == OK


def test_diagnose_thermal_missing_alerts_field():
    level, msg = diagnose_thermal({})
    assert level == OK
    assert 'no thermal' in msg


def test_diagnose_thermal_max_level_wins():
    level, _ = diagnose_thermal({'alerts': {
        'thermal_throttle': True,
        'thermal_shutdown': True,
    }})
    assert level == ERROR


# --- diagnose_alerts ---

def test_diagnose_alerts_none_active():
    level, msg, active, new_unknown = diagnose_alerts(
        {'alerts': {'motors_stuck': False, 'roaming': False}},
        ALERT_LEVEL_MAP.keys(),
        set(),
    )
    assert level == OK
    assert active == []
    assert new_unknown == []
    assert 'no alerts' in msg


def test_diagnose_alerts_known_error():
    level, msg, active, _ = diagnose_alerts(
        {'alerts': {'motors_stuck': True}},
        ALERT_LEVEL_MAP.keys(),
        set(),
    )
    assert level == ERROR
    assert active == ['motors_stuck']
    assert 'motors_stuck' in msg


def test_diagnose_alerts_skips_thermal():
    """Thermal alerts are owned by the thermal task; alerts task must ignore them."""
    level, _, active, _ = diagnose_alerts(
        {'alerts': {
            'thermal_shutdown': True,
            'unexpected_location': True,
        }},
        ALERT_LEVEL_MAP.keys(),
        set(),
    )
    # unexpected_location is WARN; thermal_shutdown must not contribute.
    assert level == WARN
    assert 'thermal_shutdown' not in active
    assert 'unexpected_location' in active


def test_diagnose_alerts_unknown_reports_once():
    seen = set()
    level1, _, _, new1 = diagnose_alerts(
        {'alerts': {'brand_new_alert': True}},
        ALERT_LEVEL_MAP.keys(),
        seen,
    )
    assert level1 == WARN
    assert new1 == ['brand_new_alert']

    seen.update(new1)

    level2, _, _, new2 = diagnose_alerts(
        {'alerts': {'brand_new_alert': True}},
        ALERT_LEVEL_MAP.keys(),
        seen,
    )
    assert level2 == WARN
    assert new2 == []  # not re-reported


def test_diagnose_alerts_install_pending_is_warn():
    level, _, active, _ = diagnose_alerts(
        {'alerts': {'install_pending': True}},
        ALERT_LEVEL_MAP.keys(),
        set(),
    )
    assert level == WARN
    assert 'install_pending' in active


def test_diagnose_alerts_multiple_active_max_level_wins():
    level, _, active, _ = diagnose_alerts(
        {'alerts': {
            'roaming': True,            # OK informational
            'slow_ethernet_speeds': True,  # WARN
            'motors_stuck': True,       # ERROR
        }},
        ALERT_LEVEL_MAP.keys(),
        set(),
    )
    assert level == ERROR
    assert 'motors_stuck' in active
    assert 'slow_ethernet_speeds' in active


def test_diagnose_alerts_missing_alerts_field():
    level, _, active, _ = diagnose_alerts({}, ALERT_LEVEL_MAP.keys(), set())
    assert level == OK
    assert active == []


# --- Table sanity ---

def test_alert_level_map_values_are_valid():
    for name, level in ALERT_LEVEL_MAP.items():
        assert level in (OK, WARN, ERROR), f'{name}={level}'


def test_state_level_map_values_are_valid():
    for name, level in STATE_LEVEL_MAP.items():
        assert level in (OK, WARN, ERROR), f'{name}={level}'


def test_thermal_alerts_all_have_entries_in_level_map():
    for name in THERMAL_ALERTS:
        assert name in ALERT_LEVEL_MAP, name


def test_thresholds_defaults_sane():
    t = Thresholds()
    assert t.obstruction_warn_fraction < t.obstruction_error_fraction
    assert t.ping_drop_warn_rate < t.ping_drop_error_rate
    assert t.ping_latency_warn_ms < t.ping_latency_error_ms
    assert t.searching_warn_delay_sec > 0
    assert t.stale_timeout_sec > 0


# --- Sparse schema resilience ---
#
# Dish hardware variants (Gen1, Gen2, Mini) and firmware versions may omit
# entire field subtrees. Every diagnose_* function must return OK (not WARN
# or ERROR) when its expected fields are absent.

class TestSparseSchemas:
    """All tasks degrade gracefully on minimal or empty responses."""

    # A response with only device_info — no state, no link metrics, no
    # alerts, no obstruction stats. Simulates a firmware that dropped
    # most of the fields the node expects.
    MINIMAL_STATUS = {
        'device_info': {
            'id': 'ut01000000-00000000-test1234',
            'hardware_version': 'rev4_prod3',
            'software_version': '2026.99.0.mr00000',
        },
    }

    def test_state_minimal(self):
        level, msg, _ = diagnose_state(
            self.MINIMAL_STATUS, STATE_LEVEL_MAP.keys(), set(),
        )
        assert level == OK
        assert 'not reported' in msg

    def test_link_minimal(self):
        level, msg = diagnose_link(
            self.MINIMAL_STATUS, Thresholds(), None, 100.0,
        )
        assert level == OK
        assert 'no link data' in msg

    def test_obstruction_minimal(self):
        level, msg = diagnose_obstruction(self.MINIMAL_STATUS, Thresholds())
        assert level == OK
        assert 'unavailable' in msg

    def test_thermal_minimal(self):
        level, _ = diagnose_thermal(self.MINIMAL_STATUS)
        assert level == OK

    def test_alerts_minimal(self):
        level, _, active, _ = diagnose_alerts(
            self.MINIMAL_STATUS, ALERT_LEVEL_MAP.keys(), set(),
        )
        assert level == OK
        assert active == []

    def test_all_tasks_ok_on_empty_status(self):
        """Completely empty status dict — every task should be OK."""
        empty = {}
        assert diagnose_state(empty, STATE_LEVEL_MAP.keys(), set())[0] == OK
        assert diagnose_link(empty, Thresholds(), None, 0.0)[0] == OK
        assert diagnose_obstruction(empty, Thresholds())[0] == OK
        assert diagnose_thermal(empty)[0] == OK
        assert diagnose_alerts(empty, ALERT_LEVEL_MAP.keys(), set())[0] == OK

    def test_state_with_link_metrics_but_no_state(self):
        """Dish reports link quality but not state enum — state task is OK."""
        status = {
            'pop_ping_drop_rate': 0.001,
            'pop_ping_latency_ms': 25.0,
            'snr_above_noise_floor': 12.0,
        }
        level, _, _ = diagnose_state(status, STATE_LEVEL_MAP.keys(), set())
        assert level == OK

    def test_link_with_partial_metrics(self):
        """Only latency reported — other metrics absent, no crash or WARN."""
        status = {'pop_ping_latency_ms': 42.0}
        level, msg = diagnose_link(status, Thresholds(), None, 100.0)
        assert level == OK
        assert '42ms' in msg

    def test_obstruction_with_fraction_but_no_currently(self):
        """fraction_obstructed present but currently_obstructed absent."""
        status = {'obstruction_stats': {'fraction_obstructed': 0.001}}
        level, msg = diagnose_obstruction(status, Thresholds())
        assert level == OK
        assert '0.10%' in msg

    def test_alerts_with_empty_alerts_dict(self):
        """Empty alerts dict — no active alerts."""
        level, msg, active, _ = diagnose_alerts(
            {'alerts': {}}, ALERT_LEVEL_MAP.keys(), set(),
        )
        assert level == OK
        assert active == []
