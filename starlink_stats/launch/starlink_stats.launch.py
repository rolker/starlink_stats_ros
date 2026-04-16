# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    args = [
        DeclareLaunchArgument(
            'dish_address', default_value='192.168.100.1:9200',
            description='Starlink dish gRPC address (host:port).',
        ),
        DeclareLaunchArgument(
            'poll_rate', default_value='1.0',
            description='Status polling rate in Hz.',
        ),
        DeclareLaunchArgument(
            'hardware_id', default_value='',
            description='Hardware ID suffix appended to diagnostic names.',
        ),
        DeclareLaunchArgument(
            'grpc_timeout_sec', default_value='2.0',
            description='Per-request gRPC timeout. Must be < 1/poll_rate to avoid backlog.',
        ),
        DeclareLaunchArgument(
            'stale_timeout_sec', default_value='5.0',
            description='Age beyond which cached status is published as STALE.',
        ),
        DeclareLaunchArgument(
            'searching_warn_delay_sec', default_value='60.0',
            description='Seconds in SEARCHING state before escalating the link task to WARN.',
        ),
        DeclareLaunchArgument(
            'obstruction_warn_fraction', default_value='0.005',
            description='fraction_obstructed threshold for WARN.',
        ),
        DeclareLaunchArgument(
            'obstruction_error_fraction', default_value='0.05',
            description='fraction_obstructed threshold for ERROR.',
        ),
        DeclareLaunchArgument(
            'ping_drop_warn_rate', default_value='0.01',
            description='pop_ping_drop_rate threshold for WARN.',
        ),
        DeclareLaunchArgument(
            'ping_drop_error_rate', default_value='0.10',
            description='pop_ping_drop_rate threshold for ERROR.',
        ),
        DeclareLaunchArgument(
            'ping_latency_warn_ms', default_value='100.0',
            description='pop_ping_latency_ms threshold for WARN.',
        ),
        DeclareLaunchArgument(
            'ping_latency_error_ms', default_value='500.0',
            description='pop_ping_latency_ms threshold for ERROR.',
        ),
        DeclareLaunchArgument(
            'snr_warn_db', default_value='9.0',
            description='snr_above_noise_floor threshold (dB) for WARN.',
        ),
        DeclareLaunchArgument(
            'dump_all_fields', default_value='false',
            description='If true, append a full flattened dump of the response as KeyValues.',
        ),
    ]

    parameters = [{
        arg.name: LaunchConfiguration(arg.name) for arg in args
    }]

    return LaunchDescription([
        *args,
        Node(
            package='starlink_stats',
            executable='starlink_diagnostics_node',
            name='starlink_diagnostics',
            parameters=parameters,
            output='screen',
        ),
    ])
