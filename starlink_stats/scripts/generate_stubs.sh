#!/usr/bin/env bash
# Generate Starlink gRPC Python stubs from the dish's reflection service.
#
# Creates a temporary venv, installs grpcio-tools and yagrc, pulls the
# protobuf descriptors via gRPC reflection, generates Python stubs, and
# cleans up the venv.  The stubs are written into the starlink_stats
# package directory as spacex_api/ so they are importable after colcon
# build.
#
# Prerequisites: python3-venv and protobuf-compiler (protoc) must be
# installed on the system.
#
# Usage:
#   ./scripts/generate_stubs.sh [DISH_ADDRESS]
#
# DISH_ADDRESS defaults to 192.168.100.1:9200.

set -euo pipefail

DISH_ADDRESS="${1:-192.168.100.1:9200}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(dirname "$SCRIPT_DIR")"
STUB_DIR="$PKG_DIR/spacex_api"
VENV_DIR=""
PROTOSET_DIR=""
PROTO_OUT=""

cleanup() {
    for d in "$VENV_DIR" "$PROTOSET_DIR" "$PROTO_OUT"; do
        if [ -n "$d" ] && [ -d "$d" ]; then
            rm -rf "$d"
        fi
    done
}
trap cleanup EXIT

# Preflight checks
for cmd in python3 protoc; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd not found. Install python3-venv and protobuf-compiler." >&2
        exit 1
    fi
done

# Detect system protobuf version before venv activation
SYSTEM_PROTOBUF_VER=$(python3 -c "import google.protobuf; print(google.protobuf.__version__)" 2>/dev/null || echo "")

echo "=== Starlink gRPC Stub Generator ==="
echo "Dish address: $DISH_ADDRESS"
echo "Output:       $STUB_DIR/"
echo ""

# Create temporary venv
VENV_DIR="$(mktemp -d /tmp/starlink_stubs_venv.XXXXXX)"
echo "Creating temporary venv in $VENV_DIR..."
python3 -m venv "$VENV_DIR"
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
echo "System protobuf version: ${SYSTEM_PROTOBUF_VER:-not found}"
echo "Installing grpcio-tools and yagrc..."
if [ -n "$SYSTEM_PROTOBUF_VER" ]; then
    pip install --quiet "grpcio-tools<2" "protobuf==$SYSTEM_PROTOBUF_VER" yagrc
else
    pip install --quiet grpcio-tools yagrc
fi

# Pull protoset from dish via reflection
PROTOSET_DIR="$(mktemp -d /tmp/starlink_protoset.XXXXXX)"
echo "Fetching protobuf descriptors from $DISH_ADDRESS..."
python3 - "$DISH_ADDRESS" "$PROTOSET_DIR" <<'PYEOF'
import sys
import grpc
from yagrc import dump

target = sys.argv[1]
outdir = sys.argv[2]

with grpc.insecure_channel(target) as channel:
    protoset = dump.dump_protocols(channel)

outpath = f"{outdir}/starlink.protoset"
with open(outpath, "wb") as f:
    f.write(protoset)

print(f"Wrote protoset ({len(protoset)} bytes) to {outpath}")
PYEOF

PROTOSET_FILE="$PROTOSET_DIR/starlink.protoset"
if [ ! -f "$PROTOSET_FILE" ]; then
    echo "Error: Failed to fetch protoset from dish." >&2
    exit 1
fi

echo "Generating Python stubs..."
PROTO_OUT="$(mktemp -d /tmp/starlink_proto_out.XXXXXX)"

# Generate Python gRPC stubs from the protoset
python3 -m grpc_tools.protoc \
    --descriptor_set_in="$PROTOSET_FILE" \
    --python_out="$PROTO_OUT" \
    --grpc_python_out="$PROTO_OUT" \
    $(python3 - "$PROTOSET_FILE" <<'PYEOF'
import sys
from google.protobuf import descriptor_pb2

with open(sys.argv[1], "rb") as f:
    fds = descriptor_pb2.FileDescriptorSet.FromString(f.read())

for fd in fds.file:
    print(fd.name)
PYEOF
)

# Clean up protoset
rm -rf "$PROTOSET_DIR"

# Deactivate venv before cleanup (trap handles removal)
deactivate

# Move generated stubs into place
if [ -d "$STUB_DIR" ]; then
    echo "Removing existing stubs..."
    rm -rf "$STUB_DIR"
fi

# The generated stubs land in spacex_api/ inside the protoc output
if [ -d "$PROTO_OUT/spacex_api" ]; then
    cp -r "$PROTO_OUT/spacex_api" "$STUB_DIR"
else
    echo "Error: Expected spacex_api/ directory not found in protoc output." >&2
    echo "Contents of output directory:" >&2
    ls -R "$PROTO_OUT" >&2
    rm -rf "$PROTO_OUT"
    exit 1
fi
rm -rf "$PROTO_OUT"

# Ensure all directories have __init__.py for Python imports
find "$STUB_DIR" -type d -exec touch {}/__init__.py \;

echo ""
echo "=== Done ==="
echo "Stubs written to: $STUB_DIR/"
echo ""
echo "Rebuild the package to install the stubs:"
echo "  colcon build --symlink-install --packages-select starlink_stats"
