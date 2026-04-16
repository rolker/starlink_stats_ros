# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
ROS 2 node that queries Starlink dish gRPC status and publishes diagnostics.

Polling runs on its own timer and caches the latest response. A separate
``diagnostic_updater.Updater`` publishes at a steady cadence regardless of
whether a new poll has completed, so rqt_robot_monitor never greys entries
out due to a slow or hung gRPC call. Staleness is reported in-band via
``DiagnosticStatus.STALE``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Optional

from diagnostic_msgs.msg import DiagnosticStatus
import diagnostic_updater
from google.protobuf.json_format import MessageToDict
import grpc
import rclpy
from rclpy.node import Node

from starlink_stats.diagnostics_logic import (
    ALERT_LEVEL_MAP,
    CURATED_KEYS,
    diagnose_alerts,
    diagnose_link,
    diagnose_obstruction,
    diagnose_state,
    diagnose_thermal,
    flatten,
    get_dotted,
    STATE_LEVEL_MAP,
    Thresholds,
)

try:
    from spacex_api.device import device_pb2
    from spacex_api.device import device_pb2_grpc
    _HAS_SPACEX_API = True
except ImportError:
    device_pb2 = None
    device_pb2_grpc = None
    _HAS_SPACEX_API = False


DISH_GET_STATUS_KEY = 'dish_get_status'


@dataclass
class CachedStatus:
    """Latest dish status response plus metadata."""

    status: Optional[dict] = None  # the dish_get_status sub-dict
    response: Optional[dict] = None  # full response (used for dump_all_fields)
    poll_monotonic: float = 0.0  # time.monotonic() at last successful poll
    poll_wall_iso: str = ''  # ISO-8601 UTC at last successful poll
    error_message: Optional[str] = None  # non-None if last poll attempt failed
    first_searching_monotonic: Optional[float] = None  # tracks SEARCHING entry


class StarlinkDiagnosticsNode(Node):
    """Polls Starlink dish via gRPC and publishes a steady-cadence diagnostic stream."""

    # Backoff grows as 2**min(attempts, _BACKOFF_CAP_EXP) and caps at _BACKOFF_MAX_SEC.
    _BACKOFF_CAP_EXP = 6
    _BACKOFF_MAX_SEC = 60.0

    def __init__(self):
        super().__init__('starlink_diagnostics')

        # Parameters
        self.declare_parameter('dish_address', '192.168.100.1:9200')
        self.declare_parameter('poll_rate', 1.0)
        self.declare_parameter('hardware_id', '')
        self.declare_parameter('grpc_timeout_sec', 2.0)
        self.declare_parameter('stale_timeout_sec', 5.0)
        self.declare_parameter('searching_warn_delay_sec', 60.0)
        self.declare_parameter('obstruction_warn_fraction', 0.005)
        self.declare_parameter('obstruction_error_fraction', 0.05)
        self.declare_parameter('ping_drop_warn_rate', 0.01)
        self.declare_parameter('ping_drop_error_rate', 0.10)
        self.declare_parameter('ping_latency_warn_ms', 100.0)
        self.declare_parameter('ping_latency_error_ms', 500.0)
        self.declare_parameter('snr_warn_db', 9.0)
        self.declare_parameter('dump_all_fields', False)

        self.dish_address = self.get_parameter('dish_address').value
        poll_rate = float(self.get_parameter('poll_rate').value)
        self.hardware_id = self.get_parameter('hardware_id').value
        self.grpc_timeout_sec = float(self.get_parameter('grpc_timeout_sec').value)
        self.dump_all_fields = bool(self.get_parameter('dump_all_fields').value)

        def _p(name: str) -> float:
            return float(self.get_parameter(name).value)

        self.thresholds = Thresholds(
            obstruction_warn_fraction=_p('obstruction_warn_fraction'),
            obstruction_error_fraction=_p('obstruction_error_fraction'),
            ping_drop_warn_rate=_p('ping_drop_warn_rate'),
            ping_drop_error_rate=_p('ping_drop_error_rate'),
            ping_latency_warn_ms=_p('ping_latency_warn_ms'),
            ping_latency_error_ms=_p('ping_latency_error_ms'),
            snr_warn_db=_p('snr_warn_db'),
            searching_warn_delay_sec=_p('searching_warn_delay_sec'),
            stale_timeout_sec=_p('stale_timeout_sec'),
        )

        # Cached state + gRPC bookkeeping
        self._cache = CachedStatus()
        self._channel = None
        self._stub = None
        self._reconnect_attempts = 0
        self._reconnect_backoff_sec = 0.0
        self._next_reconnect_monotonic = 0.0
        self._unknown_states_seen: set[str] = set()
        self._unknown_alerts_seen: set[str] = set()

        # Updater — steady 1 Hz publishing decoupled from polling.
        self._name_prefix = (
            f'Starlink: {self.hardware_id}' if self.hardware_id else 'Starlink'
        )
        self._updater = diagnostic_updater.Updater(self, period=1.0)
        self._updater.setHardwareID(self.hardware_id if self.hardware_id else 'none')
        self._updater.add(f'{self._name_prefix}: comms', self._task_comms)
        self._updater.add(f'{self._name_prefix}: state', self._task_state)
        self._updater.add(f'{self._name_prefix}: link', self._task_link)
        self._updater.add(f'{self._name_prefix}: obstruction', self._task_obstruction)
        self._updater.add(f'{self._name_prefix}: thermal', self._task_thermal)
        self._updater.add(f'{self._name_prefix}: alerts', self._task_alerts)

        # Poll timer — independent of Updater period.
        self._connect()
        poll_period = 1.0 / max(poll_rate, 0.01)
        self._poll_timer = self.create_timer(poll_period, self._poll_callback)

        self.get_logger().info(
            f'Starlink diagnostics node started, polling {self.dish_address} '
            f'at {poll_rate} Hz (grpc_timeout={self.grpc_timeout_sec}s)'
            + (f', hardware_id={self.hardware_id}' if self.hardware_id else '')
        )

    # --- gRPC channel management ---

    def _connect(self):
        """Create or recreate the gRPC channel and stub."""
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        self._channel = grpc.insecure_channel(self.dish_address)
        if _HAS_SPACEX_API:
            self._stub = device_pb2_grpc.DeviceStub(self._channel)
        else:
            self._stub = None

    def _schedule_reconnect(self, now_monotonic: float):
        """Bump exponential backoff and set the next allowed reconnect time."""
        self._reconnect_attempts += 1
        exp = min(self._reconnect_attempts, self._BACKOFF_CAP_EXP)
        self._reconnect_backoff_sec = min(self._BACKOFF_MAX_SEC, float(2 ** exp))
        self._next_reconnect_monotonic = now_monotonic + self._reconnect_backoff_sec

    # --- Poll cycle ---

    def _poll_callback(self):
        """Query dish and update cache. Never blocks the Updater."""
        now_mono = time.monotonic()

        if not _HAS_SPACEX_API:
            self._cache.error_message = 'spacex_api protobuf modules not found'
            self._cache.status = None
            return

        # Respect backoff window after a failure.
        if self._reconnect_attempts > 0 and now_mono < self._next_reconnect_monotonic:
            return

        try:
            response = self._stub.Handle(
                device_pb2.Request(get_status={}),
                timeout=self.grpc_timeout_sec,
            )
        except grpc.RpcError as e:
            code_name = 'unknown'
            if hasattr(e, 'code') and callable(e.code):
                code = e.code()
                if code is not None:
                    code_name = code.name
            self._cache.error_message = f'dish unreachable ({code_name})'
            self._cache.status = None
            self._schedule_reconnect(now_mono)
            # Recreate channel for the next attempt (after backoff expires).
            try:
                self._connect()
            except Exception as ex:  # pragma: no cover - grpc internals
                self.get_logger().warning(f'reconnect failed: {ex}')
            return

        full = MessageToDict(
            response,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        status = full.get(DISH_GET_STATUS_KEY)
        if not isinstance(status, dict):
            # Unexpected oneof - fall back to the full response so downstream
            # tasks still have something to look at.
            status = full

        self._cache.status = status
        self._cache.response = full
        self._cache.poll_monotonic = now_mono
        self._cache.poll_wall_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        self._cache.error_message = None

        # Track SEARCHING entry so the link task can apply the grace window.
        state = status.get('state') if isinstance(status, dict) else None
        if state == 'SEARCHING':
            if self._cache.first_searching_monotonic is None:
                self._cache.first_searching_monotonic = now_mono
        else:
            self._cache.first_searching_monotonic = None

        # Reset backoff on success.
        self._reconnect_attempts = 0
        self._reconnect_backoff_sec = 0.0

    # --- Updater task helpers ---

    def _data_age(self, now_mono: float) -> Optional[float]:
        if self._cache.poll_monotonic <= 0.0:
            return None
        return now_mono - self._cache.poll_monotonic

    def _is_stale(self, now_mono: float) -> bool:
        age = self._data_age(now_mono)
        return age is None or age > self.thresholds.stale_timeout_sec

    def _apply_common_kv(self, stat, task_name: str):
        """Append shared KeyValues: last_query_time and either curated or dumped fields."""
        stat.add('last_query_time', self._cache.poll_wall_iso or 'never')
        if self.dump_all_fields and self._cache.response is not None:
            for k, v in flatten(self._cache.response).items():
                stat.add(k, str(v))
            return
        status = self._cache.status or {}
        for path in CURATED_KEYS.get(task_name, []):
            val = get_dotted(status, path)
            if val is not None:
                stat.add(path, str(val))

    # --- Task callbacks ---

    def _task_comms(self, stat):
        """Report gRPC reachability, last-query age, and reconnect state."""
        now_mono = time.monotonic()
        if self._cache.error_message and self._cache.status is None:
            stat.summary(DiagnosticStatus.ERROR, self._cache.error_message)
        elif self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no successful poll yet')
        elif self._is_stale(now_mono):
            age = self._data_age(now_mono) or 0.0
            stat.summary(
                DiagnosticStatus.STALE,
                f'no response for {age:.1f}s '
                f'(stale_timeout_sec={self.thresholds.stale_timeout_sec})',
            )
        else:
            age = self._data_age(now_mono) or 0.0
            stat.summary(DiagnosticStatus.OK, f'dish reachable ({age:.1f}s ago)')

        self._apply_common_kv(stat, 'comms')
        if self._reconnect_attempts > 0:
            stat.add('reconnect_attempts', str(self._reconnect_attempts))
            stat.add('reconnect_backoff_sec', f'{self._reconnect_backoff_sec:.1f}')
        return stat

    def _task_state(self, stat):
        if self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, 'state')
            return stat
        level, msg, new_unknown = diagnose_state(
            self._cache.status,
            STATE_LEVEL_MAP.keys(),
            self._unknown_states_seen,
        )
        for name in new_unknown:
            self._unknown_states_seen.add(name)
            self.get_logger().warning(f'Starlink: unknown status.state value: {name}')
        stat.summary(level, msg)
        self._apply_common_kv(stat, 'state')
        return stat

    def _task_link(self, stat):
        if self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, 'link')
            return stat
        level, msg = diagnose_link(
            self._cache.status,
            self.thresholds,
            self._cache.first_searching_monotonic,
            time.monotonic(),
        )
        stat.summary(level, msg)
        self._apply_common_kv(stat, 'link')
        return stat

    def _task_obstruction(self, stat):
        if self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, 'obstruction')
            return stat
        level, msg = diagnose_obstruction(self._cache.status, self.thresholds)
        stat.summary(level, msg)
        self._apply_common_kv(stat, 'obstruction')
        return stat

    def _task_thermal(self, stat):
        if self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, 'thermal')
            return stat
        level, msg = diagnose_thermal(self._cache.status)
        stat.summary(level, msg)
        self._apply_common_kv(stat, 'thermal')
        return stat

    def _task_alerts(self, stat):
        if self._cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, 'alerts')
            return stat
        level, msg, active, new_unknown = diagnose_alerts(
            self._cache.status,
            ALERT_LEVEL_MAP.keys(),
            self._unknown_alerts_seen,
        )
        for name in new_unknown:
            self._unknown_alerts_seen.add(name)
            self.get_logger().warning(f'Starlink: unknown alert name: {name}')
        stat.summary(level, msg)
        # Only active alerts are useful as KeyValues; the others are always false.
        for name in active:
            stat.add(f'alerts.{name}', 'true')
        stat.add('last_query_time', self._cache.poll_wall_iso or 'never')
        return stat

    def destroy_node(self):
        if self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = StarlinkDiagnosticsNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
