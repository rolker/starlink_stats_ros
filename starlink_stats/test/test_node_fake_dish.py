# Copyright 2026 University of New Hampshire
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
End-to-end tests: the diagnostics node against a fake dish over real gRPC.

The fake dish's schema is a stand-in (see fake_dish.py). These tests check
the node's plumbing - reflection, polling, history, telemetry topics,
diagnostics, firmware changes - not the meaning of real dish fields, which
needs a live dish.
"""

import json
import random
import time

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus
from fake_dish import build_fds, FakeDish
import pytest
import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.parameter import Parameter

from starlink_stats import grpc_reflection
from starlink_stats import starlink_diagnostics_node
from starlink_stats.starlink_diagnostics_node import StarlinkDiagnosticsNode
from std_msgs.msg import String

NEW_FW = '2026.09.01.test.1'


@pytest.fixture
def ros(tmp_path, monkeypatch):
    """Isolate the descriptor cache and give each test its own ROS context."""
    monkeypatch.setattr(grpc_reflection, 'CACHE_DIR', tmp_path / 'cache')
    # A domain of its own, so no other ROS process on the host reaches
    # /diagnostics here.
    rclpy.init(domain_id=random.randint(150, 230))
    yield tmp_path / 'cache'
    rclpy.shutdown()


class Harness:
    """Run the node and a listener on one executor; collect what they publish."""

    def __init__(self, dish, **params):
        overrides = {'dish_address': dish.address, 'poll_rate': 10.0}
        overrides.update(params)
        self.node = StarlinkDiagnosticsNode(parameter_overrides=[
            Parameter(k, value=v) for k, v in overrides.items()])
        self.listener = rclpy.create_node('fake_dish_listener')
        self.status_json: list[dict] = []
        self.history_json: list[dict] = []
        self.diag: dict[str, DiagnosticStatus] = {}
        self.listener.create_subscription(
            String, '/starlink_diagnostics/status_json',
            lambda m: self.status_json.append(json.loads(m.data)), 10)
        self.listener.create_subscription(
            String, '/starlink_diagnostics/history_json',
            lambda m: self.history_json.append(json.loads(m.data)), 10)
        self.listener.create_subscription(
            DiagnosticArray, '/diagnostics', self._on_diag, 10)
        self.executor = SingleThreadedExecutor()
        self.executor.add_node(self.node)
        self.executor.add_node(self.listener)

    def _on_diag(self, msg):
        for status in msg.status:
            self.diag[status.name.rsplit(': ', 1)[-1]] = status

    def spin_until(self, predicate, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.executor.spin_once(timeout_sec=0.05)
            if predicate():
                return True
        return False

    def kv(self, task, key):
        status = self.diag.get(task)
        if status is None:
            return None
        return next((v.value for v in status.values if v.key == key), None)

    def close(self):
        self.executor.shutdown()
        self.node.destroy_node()
        self.listener.destroy_node()


@pytest.fixture
def dish():
    d = FakeDish().start()
    yield d
    d.stop()


def test_publishes_status_and_history_json(ros, dish):
    h = Harness(dish, publish_telemetry=True)
    try:
        assert h.spin_until(lambda: h.status_json and h.history_json)
        status = h.status_json[-1]
        assert 'query_time_utc' in status
        info = status['response']['dish_get_status']['device_info']
        assert info['hardware_version'] == 'mini1_prod2'
        history = h.history_json[-1]['response']['dish_get_history']
        assert len(history['power_in']) == 900
        # One history fetch per period, not one per status poll.
        h.spin_until(lambda: False, timeout=1.0)
        assert dish.requests.get('get_history') == 1
        assert dish.requests.get('get_status', 0) > 3
    finally:
        h.close()


def test_telemetry_off_by_default_but_history_still_feeds_diagnostics(ros, dish):
    h = Harness(dish)
    try:
        assert h.spin_until(lambda: h.kv('state', 'power_in_w') is not None)
        assert h.kv('state', 'power_in_w') == '77.5'
        assert h.node.count_publishers('/starlink_diagnostics/status_json') == 0
        assert h.status_json == [] and h.history_json == []
    finally:
        h.close()


def test_mini_readiness_and_pointing_diagnostics(ros, dish):
    h = Harness(dish)
    try:
        assert h.spin_until(lambda: 'pointing' in h.diag and 'state' in h.diag
                            and h.diag['state'].message != 'no data')
        assert h.diag['state'].level == DiagnosticStatus.OK
        assert h.diag['state'].message == 'all subsystems ready'
        assert h.diag['pointing'].level == DiagnosticStatus.OK
        assert h.diag['pointing'].message == 'GPS fix (14 sats), tilt 3.5 deg'
        assert h.kv('pointing', 'gps_stats.gps_sats') == '14'

        dish.status['ready_states']['rf'] = False
        dish.status['gps_stats'] = {'gps_valid': False, 'gps_sats': 0}
        assert h.spin_until(lambda: h.diag['state'].message == 'not ready: rf')
        assert h.diag['state'].level == DiagnosticStatus.WARN
        assert h.spin_until(
            lambda: h.diag['pointing'].level == DiagnosticStatus.WARN)
    finally:
        h.close()


def test_firmware_without_history_keeps_polling_status(ros):
    dish = FakeDish(build_fds(with_history=False)).start()
    h = Harness(dish, publish_telemetry=True)
    try:
        assert h.spin_until(lambda: len(h.status_json) > 5)
        assert 'get_history' not in dish.requests
        assert h.history_json == []
        assert h.node._history_unsupported

        # A firmware update that adds history turns it back on.
        dish.set_schema(build_fds())
        dish.status['device_info']['software_version'] = NEW_FW
        assert h.spin_until(lambda: h.history_json)
    finally:
        h.close()
        dish.stop()


def test_firmware_update_with_schema_change_is_picked_up(ros, dish):
    """Regression: a changed schema under the same file name used to wedge polling."""
    h = Harness(dish, publish_telemetry=True)
    try:
        assert h.spin_until(lambda: h.status_json)
        dish.set_schema(build_fds(extra_status_field='new_firmware_field'))
        dish.status['device_info']['software_version'] = NEW_FW
        dish.status['new_firmware_field'] = 1.5

        def has_new_field():
            return any(
                'new_firmware_field' in m['response']['dish_get_status']
                for m in h.status_json)
        assert h.spin_until(has_new_field)
        # The cache is saved by the same poll, after it publishes.
        assert h.spin_until(
            lambda: grpc_reflection.cached_firmware_version() == NEW_FW)
    finally:
        h.close()


def test_stale_cached_schema_from_older_firmware_is_replaced(ros, dish):
    """Descriptors cached under other firmware are re-reflected at startup."""
    grpc_reflection.save_cached_fds(build_fds(), 'older-firmware')
    dish.set_schema(build_fds(extra_status_field='new_firmware_field'))
    dish.status['device_info']['software_version'] = NEW_FW
    dish.status['new_firmware_field'] = 1.5
    h = Harness(dish, publish_telemetry=True)
    try:
        def has_new_field():
            return any(
                'new_firmware_field' in m['response']['dish_get_status']
                for m in h.status_json)
        assert h.spin_until(has_new_field)
        # The cache is saved by the same poll, after it publishes.
        assert h.spin_until(
            lambda: grpc_reflection.cached_firmware_version() == NEW_FW)
    finally:
        h.close()


def test_unreachable_dish_publishes_no_telemetry(ros, dish):
    dish.fail = True
    h = Harness(dish, publish_telemetry=True)
    try:
        assert h.spin_until(
            lambda: 'comms' in h.diag
            and h.diag['comms'].level == DiagnosticStatus.ERROR)
        assert h.status_json == [] and h.history_json == []
    finally:
        h.close()


def test_history_period_zero_disables_history(ros, dish):
    h = Harness(dish, publish_telemetry=True, history_period_sec=0.0)
    try:
        assert h.spin_until(lambda: len(h.status_json) > 5)
        assert 'get_history' not in dish.requests
        assert h.kv('state', 'power_in_w') is None
    finally:
        h.close()


def test_history_waits_while_the_dish_is_unreachable(ros, dish):
    h = Harness(dish, publish_telemetry=True)
    try:
        assert h.spin_until(lambda: h.history_json)
        h.node.history_period_sec = 0.5  # make the next fetch due quickly
        h.node._history_due_monotonic = 0.0
        dish.fail = True
        assert h.spin_until(lambda: h.diag.get('comms') is not None
                            and h.diag['comms'].level == DiagnosticStatus.ERROR)
        during = dish.requests.get('get_history', 0)
        h.spin_until(lambda: False, timeout=1.5)
        assert dish.requests.get('get_history', 0) == during
        dish.fail = False
        assert h.spin_until(lambda: dish.requests.get('get_history', 0) > during,
                            timeout=15.0)
    finally:
        h.close()


def test_failed_history_is_retried_and_stale_power_is_dropped(ros, dish, monkeypatch):
    monkeypatch.setattr(starlink_diagnostics_node, '_HISTORY_RETRY_SEC', 0.3)
    h = Harness(dish, history_period_sec=1.0)
    try:
        assert h.spin_until(lambda: h.kv('state', 'power_in_w') == '77.5')
        dish.fail_history = True
        first = dish.requests.get('get_history', 0)
        # Retried after each failure...
        assert h.spin_until(lambda: dish.requests.get('get_history', 0) >= first + 3)
        # ...and once two periods pass without a fetch, the value is dropped.
        assert h.spin_until(lambda: h.kv('state', 'power_in_w') is None
                            and h.diag['state'].level == DiagnosticStatus.OK)
        dish.fail_history = False
        assert h.spin_until(lambda: h.kv('state', 'power_in_w') == '77.5')
    finally:
        h.close()
