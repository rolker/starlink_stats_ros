# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""ROS 2 node that queries Starlink dish gRPC status and publishes diagnostics."""

from collections.abc import MutableMapping

from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from google.protobuf.json_format import MessageToDict
import grpc
import rclpy
from rclpy.node import Node


def flatten(d, parent_key='', sep='_'):
    """Flatten a nested dictionary into a single-level dict with joined keys."""
    items = []
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


class StarlinkDiagnosticsNode(Node):

    def __init__(self):
        super().__init__('starlink_diagnostics')

        self.declare_parameter('dish_address', '192.168.100.1:9200')
        self.declare_parameter('poll_rate', 1.0)
        self.declare_parameter('hardware_id', '')

        self.dish_address = self.get_parameter('dish_address').value
        poll_rate = self.get_parameter('poll_rate').value
        self.hardware_id = self.get_parameter('hardware_id').value

        self.pub = self.create_publisher(DiagnosticArray, '/diagnostics', 10)

        self.channel = None
        self.stub = None
        self._connect()

        self.timer = self.create_timer(1.0 / poll_rate, self.timer_callback)

        self._name_prefix = (
            f'Starlink: {self.hardware_id}' if self.hardware_id
            else 'Starlink'
        )

        self.get_logger().info(
            f'Starlink diagnostics node started, polling {self.dish_address} '
            f'at {poll_rate} Hz'
            + (f' (hardware_id: {self.hardware_id})' if self.hardware_id else '')
        )

    def _connect(self):
        """Create or recreate the gRPC channel and stub."""
        if self.channel is not None:
            self.channel.close()
        self.channel = grpc.insecure_channel(self.dish_address)
        try:
            from spacex_api.device import device_pb2_grpc
            self.stub = device_pb2_grpc.DeviceStub(self.channel)
        except ModuleNotFoundError:
            self.stub = None

    def query_dish(self):
        """Query the Starlink dish via gRPC and return status as a dict."""
        if self.stub is None:
            return {
                'Starlink': {
                    'level': DiagnosticStatus.ERROR,
                    'message': 'Generated gRPC protobuf modules not found',
                }
            }

        try:
            from spacex_api.device import device_pb2
            response = self.stub.Handle(
                device_pb2.Request(get_status={}), timeout=10
            )
        except grpc.RpcError as e:
            self._connect()
            return {
                'Starlink': {
                    'level': DiagnosticStatus.ERROR,
                    'message': f'Dish not reachable: {e}',
                }
            }

        return MessageToDict(
            response,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )

    def _check_alerts(self, status_dict):
        """Derive diagnostic level from the alerts sub-dict."""
        alerts = status_dict.get('alerts', {})
        if not isinstance(alerts, dict):
            return DiagnosticStatus.OK
        active = [k for k, v in alerts.items() if v]
        if 'thermal_shutdown' in active:
            return DiagnosticStatus.ERROR
        if active:
            return DiagnosticStatus.WARN
        return DiagnosticStatus.OK

    def timer_callback(self):
        diag_array = DiagnosticArray()
        diag_array.header.stamp = self.get_clock().now().to_msg()

        try:
            diagnostic_data = self.query_dish()

            for key, value in diagnostic_data.items():
                if not isinstance(value, dict):
                    continue

                # Error dicts from query_dish() contain 'level' and 'message'
                if 'level' in value and 'message' in value:
                    diag_status = DiagnosticStatus()
                    diag_status.name = self._name_prefix
                    diag_status.hardware_id = self.hardware_id
                    diag_status.level = int(value['level'])
                    diag_status.message = str(value['message'])
                    diag_array.status.append(diag_status)
                    continue

                diag_status = DiagnosticStatus()
                diag_status.name = f'{self._name_prefix}: {key}'
                diag_status.hardware_id = (
                    self.hardware_id
                    or str(value.get('device_info', {}).get('id', ''))
                )
                diag_status.level = self._check_alerts(value)

                flat = flatten(value)
                for k, v in flat.items():
                    kv = KeyValue()
                    kv.key = k
                    kv.value = str(v)
                    diag_status.values.append(kv)

                diag_array.status.append(diag_status)

            self.pub.publish(diag_array)

        except Exception as e:
            self.get_logger().warning(
                f'Failed to get Starlink diagnostics: {e}'
            )

    def destroy_node(self):
        """Close gRPC channel before destroying the node."""
        if self.channel is not None:
            self.channel.close()
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
