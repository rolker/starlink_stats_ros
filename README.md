# starlink_stats

## Overview

ROS 2 node that connects to a Starlink dish via gRPC, queries its diagnostic
status, and publishes it as a `diagnostic_msgs/DiagnosticArray` at a steady
1 Hz cadence. Uses `diagnostic_updater` with seven named tasks (comms, state,
link, obstruction, thermal, alerts, pointing). Optionally publishes the dish's
full status and history buffer as JSON, for recording.

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
- `dump_all_fields` — include full flattened response as KeyValues (default `false`).
  The dump is appended to every task, so it repeats seven times per update.
- `stale_timeout_sec` — age beyond which cached status is STALE (default `5.0`)
- `publish_telemetry` — publish the telemetry topics below (default `false`)
- `history_period_sec` — seconds between history-buffer fetches, `0` disables
  (default `300.0`; the dish keeps about 15 minutes). A fetch shares the
  status poll's worker and `grpc_timeout_sec`; it is skipped while the dish
  is unreachable and retried 30 s after a failure.

## Diagnostics

- **state** — the dish's `state` field when it reports one. Some dishes (the
  Mini on 2026.08 firmware) do not; the task then reports the subsystem ready
  flags (`ready_states`), WARNing with the names of any not ready. Also carries
  `power_in_w`, the dish's newest input power from the history buffer, and
  `power_in_age_s`; both are left out once two fetches in a row have been
  missed.
- **link** — ping drop rate and latency to the Starlink PoP, SNR flags.
- **obstruction**, **thermal**, **alerts** — as reported by the dish.
- **pointing** — the dish's own GPS fix (WARN with none, unless the dish
  reports its GPS deliberately inhibited) and satellite count, tilt and
  attitude uncertainty. No thresholds on tilt yet.
- **comms** — whether the node is reaching the dish.

## Telemetry topics

With `publish_telemetry: true`:

| Topic | Type | Rate |
|---|---|---|
| `~/status_json` | `std_msgs/String` | every successful status poll |
| `~/history_json` | `std_msgs/String` | every history fetch |

Each message is `{"query_time_utc": ..., "response": {...}}`: the time the
dish answered, and its full response rendered from the reflected schema, so
new firmware fields appear without a code change. These are for bags on the
boat. Each status message carries every field the dish reports (its size
depends on the firmware), once per poll; don't forward them over a
bandwidth-limited link.

The history arrays are a ring buffer: `current` counts samples written, so
the newest is at index `(current - 1) % len(array)`. That reading follows
community tools and has not been checked against this firmware.

## Tests

`colcon test` runs everything without a dish. `test/fake_dish.py` is a local
gRPC server with reflection and a stand-in schema; the node is tested
against it end to end (polling, history, telemetry topics, diagnostics,
firmware changes). The stand-in is not the dish's real schema. When a
descriptor set is captured from a dish (the node caches one under
`~/.cache/starlink_stats/`), the fake should serve that instead.

## Notes

- The device running this node must be able to reach the dish's `192.168.100.1`
  address on port 9200 (gRPC).
- If using the stock Starlink router, it typically has a route preconfigured for
  devices on its `192.168.1.x` network.
