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

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
import threading
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
        # Parameter is declared with a bool default, so rclpy enforces the
        # type. No bool() cast here because bool('false') == True would be
        # a silent bug if a non-bool value ever slipped through.
        self.dump_all_fields = self.get_parameter('dump_all_fields').value

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

        # Cached state + gRPC bookkeeping.
        # self._cache is replaced atomically (whole-dataclass swap) from the
        # worker thread so task callbacks on the rclpy thread only ever see a
        # consistent snapshot. Task callbacks should take a local reference
        # at entry and not re-read self._cache during one update.
        self._cache = CachedStatus()
        self._channel = None
        self._stub = None
        self._reconnect_attempts = 0
        self._reconnect_backoff_sec = 0.0
        self._next_reconnect_monotonic = 0.0
        self._unknown_states_seen: set[str] = set()
        self._unknown_alerts_seen: set[str] = set()

        # gRPC runs on a single-worker background thread so a slow or hung
        # call never blocks the rclpy executor (and therefore the Updater's
        # publish timer). The in-flight flag and lock prevent queueing up
        # polls if the dish is slower than the poll period.
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='starlink-grpc'
        )
        self._poll_lock = threading.Lock()
        self._poll_in_flight = False

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
        """Kick off a background gRPC poll. Runs on the rclpy executor thread."""
        now_mono = time.monotonic()

        if not _HAS_SPACEX_API:
            # Atomic cache swap; preserve SEARCHING tracking from prior state.
            prev = self._cache
            self._cache = CachedStatus(
                poll_monotonic=prev.poll_monotonic,
                poll_wall_iso=prev.poll_wall_iso,
                error_message='spacex_api protobuf modules not found',
                first_searching_monotonic=prev.first_searching_monotonic,
            )
            return

        # Respect backoff window after a failure.
        if self._reconnect_attempts > 0 and now_mono < self._next_reconnect_monotonic:
            return

        # Claim the in-flight slot; skip this tick if a previous query is
        # still running (e.g. dish is slow and poll_rate is faster than response).
        with self._poll_lock:
            if self._poll_in_flight:
                return
            self._poll_in_flight = True

        future = self._grpc_executor.submit(self._blocking_query)
        future.add_done_callback(self._handle_poll_result)

    def _blocking_query(self) -> tuple[dict, float, str]:
        """Run on the gRPC worker thread; returns parsed response + timestamps."""
        response = self._stub.Handle(
            device_pb2.Request(get_status={}),
            timeout=self.grpc_timeout_sec,
        )
        full = MessageToDict(
            response,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        now_mono = time.monotonic()
        now_iso = datetime.now(timezone.utc).isoformat(timespec='seconds')
        return full, now_mono, now_iso

    def _handle_poll_result(self, future: Future):
        """Done-callback on the gRPC worker thread. Swaps the cache atomically."""
        try:
            try:
                full, now_mono, now_iso = future.result()
            except grpc.RpcError as e:
                code_name = 'unknown'
                if hasattr(e, 'code') and callable(e.code):
                    code = e.code()
                    if code is not None:
                        code_name = code.name
                prev = self._cache
                self._cache = CachedStatus(
                    poll_monotonic=prev.poll_monotonic,
                    poll_wall_iso=prev.poll_wall_iso,
                    error_message=f'dish unreachable ({code_name})',
                    first_searching_monotonic=prev.first_searching_monotonic,
                )
                self._schedule_reconnect(time.monotonic())
                try:
                    self._connect()
                except Exception as ex:  # pragma: no cover - grpc internals
                    self.get_logger().warning(f'reconnect failed: {ex}')
                return
            except Exception as ex:  # pragma: no cover - unexpected
                self.get_logger().warning(f'poll handler error: {ex}')
                return

            status = full.get(DISH_GET_STATUS_KEY)
            if not isinstance(status, dict):
                # Unexpected oneof - fall back to the full response so
                # downstream tasks still have something to look at.
                status = full

            state = status.get('state') if isinstance(status, dict) else None
            prev = self._cache
            if state == 'SEARCHING':
                first_search = prev.first_searching_monotonic or now_mono
            else:
                first_search = None

            self._cache = CachedStatus(
                status=status,
                response=full,
                poll_monotonic=now_mono,
                poll_wall_iso=now_iso,
                error_message=None,
                first_searching_monotonic=first_search,
            )
            self._reconnect_attempts = 0
            self._reconnect_backoff_sec = 0.0
        finally:
            with self._poll_lock:
                self._poll_in_flight = False

    # --- Updater task helpers ---
    #
    # Tasks take a local snapshot (`cache = self._cache`) and pass it through
    # the helpers below so that an atomic swap mid-update (from the worker
    # thread) cannot produce a torn view of the status.

    @staticmethod
    def _data_age(cache: 'CachedStatus', now_mono: float) -> Optional[float]:
        if cache.poll_monotonic <= 0.0:
            return None
        return now_mono - cache.poll_monotonic

    def _is_stale(self, cache: 'CachedStatus', now_mono: float) -> bool:
        age = self._data_age(cache, now_mono)
        return age is None or age > self.thresholds.stale_timeout_sec

    def _apply_common_kv(self, stat, cache: 'CachedStatus', task_name: str):
        """Append shared KeyValues: last_query_time and either curated or dumped fields."""
        stat.add('last_query_time', cache.poll_wall_iso or 'never')
        if self.dump_all_fields and cache.response is not None:
            for k, v in flatten(cache.response).items():
                stat.add(k, str(v))
            return
        status = cache.status or {}
        for path in CURATED_KEYS.get(task_name, []):
            val = get_dotted(status, path)
            if val is not None:
                stat.add(path, str(val))

    # --- Task callbacks ---

    def _task_comms(self, stat):
        """Report gRPC reachability, last-query age, and reconnect state."""
        cache = self._cache  # snapshot
        now_mono = time.monotonic()
        if cache.error_message and cache.status is None:
            stat.summary(DiagnosticStatus.ERROR, cache.error_message)
        elif cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no successful poll yet')
        elif self._is_stale(cache, now_mono):
            age = self._data_age(cache, now_mono) or 0.0
            stat.summary(
                DiagnosticStatus.STALE,
                f'no response for {age:.1f}s '
                f'(stale_timeout_sec={self.thresholds.stale_timeout_sec})',
            )
        else:
            age = self._data_age(cache, now_mono) or 0.0
            stat.summary(DiagnosticStatus.OK, f'dish reachable ({age:.1f}s ago)')

        self._apply_common_kv(stat, cache, 'comms')
        if self._reconnect_attempts > 0:
            stat.add('reconnect_attempts', str(self._reconnect_attempts))
            stat.add('reconnect_backoff_sec', f'{self._reconnect_backoff_sec:.1f}')
        return stat

    def _task_state(self, stat):
        cache = self._cache
        if cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, cache, 'state')
            return stat
        level, msg, new_unknown = diagnose_state(
            cache.status,
            STATE_LEVEL_MAP.keys(),
            self._unknown_states_seen,
        )
        for name in new_unknown:
            self._unknown_states_seen.add(name)
            self.get_logger().warning(f'Starlink: unknown status.state value: {name}')
        stat.summary(level, msg)
        self._apply_common_kv(stat, cache, 'state')
        return stat

    def _task_link(self, stat):
        cache = self._cache
        if cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, cache, 'link')
            return stat
        level, msg = diagnose_link(
            cache.status,
            self.thresholds,
            cache.first_searching_monotonic,
            time.monotonic(),
        )
        stat.summary(level, msg)
        self._apply_common_kv(stat, cache, 'link')
        return stat

    def _task_obstruction(self, stat):
        cache = self._cache
        if cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, cache, 'obstruction')
            return stat
        level, msg = diagnose_obstruction(cache.status, self.thresholds)
        stat.summary(level, msg)
        self._apply_common_kv(stat, cache, 'obstruction')
        return stat

    def _task_thermal(self, stat):
        cache = self._cache
        if cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, cache, 'thermal')
            return stat
        level, msg = diagnose_thermal(cache.status)
        stat.summary(level, msg)
        self._apply_common_kv(stat, cache, 'thermal')
        return stat

    def _task_alerts(self, stat):
        cache = self._cache
        if cache.status is None:
            stat.summary(DiagnosticStatus.STALE, 'no data')
            self._apply_common_kv(stat, cache, 'alerts')
            return stat
        level, msg, active, new_unknown = diagnose_alerts(
            cache.status,
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
        stat.add('last_query_time', cache.poll_wall_iso or 'never')
        return stat

    def destroy_node(self):
        # Shut down the gRPC worker thread before closing the channel; don't
        # wait, as a hung RPC would block shutdown indefinitely.
        try:
            self._grpc_executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
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
