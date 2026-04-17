# starlink_stats

## Overview

ROS 2 node that connects to a Starlink dish via gRPC, queries its diagnostic
status, and publishes it as a `diagnostic_msgs/DiagnosticArray` at a steady
1 Hz cadence. Uses `diagnostic_updater` with six named tasks (comms, state,
link, obstruction, thermal, alerts).

## Prerequisites

- ROS 2 (Jazzy or later)
- Python 3
- Starlink dish reachable at its gRPC address (default `192.168.100.1:9200`)

## gRPC Proto Discovery

The node discovers the dish's protobuf schema **at runtime** via gRPC server
reflection — no pre-generated stubs or manual proto extraction needed. On the
first successful connection, the node:

1. Calls the dish's reflection service to fetch protobuf descriptors.
2. Builds dynamic message classes using `google.protobuf.descriptor_pool`.
3. Caches the descriptors at `~/.cache/starlink_stats/` for reuse on later
   startups, and refreshes the cache if the dish firmware version changes.

This makes the package resilient to dish hardware variants (Gen1, Gen2, Mini)
and firmware updates that change the protobuf schema.

## Installation

1. Clone this package into a ROS 2 workspace `src/` directory.
2. Build with `colcon build --packages-select starlink_stats --symlink-install`.
3. Source the workspace: `source install/setup.bash`.

No additional steps needed — the node handles proto discovery automatically.

## Usage

```bash
ros2 launch starlink_stats starlink_stats.launch.py
```

Key parameters (all configurable via launch args):

- `dish_address` — gRPC address (default `192.168.100.1:9200`)
- `hardware_id` — suffix for diagnostic names (default: auto-detected from dish)
- `poll_rate` — status polling rate in Hz (default `1.0`)
- `dump_all_fields` — include full flattened response as KeyValues (default `false`)
- `stale_timeout_sec` — age beyond which cached status is STALE (default `5.0`)

## Notes

- The device running this node must be able to reach the dish's `192.168.100.1`
  address on port 9200 (gRPC).
- If using the stock Starlink router, it typically has a route preconfigured for
  devices on its `192.168.1.x` network.
