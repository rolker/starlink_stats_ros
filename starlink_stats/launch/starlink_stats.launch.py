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
    return LaunchDescription([
        DeclareLaunchArgument(
            'dish_address',
            default_value='192.168.100.1:9200',
            description='Starlink dish gRPC address (host:port)',
        ),
        DeclareLaunchArgument(
            'poll_rate',
            default_value='1.0',
            description='Status polling rate in Hz',
        ),
        Node(
            package='starlink_stats',
            executable='starlink_diagnostics_node',
            name='starlink_diagnostics',
            parameters=[{
                'dish_address': LaunchConfiguration('dish_address'),
                'poll_rate': LaunchConfiguration('poll_rate'),
            }],
            output='screen',
        ),
    ])
