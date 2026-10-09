# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
Minimal gRPC reflection client for the Starlink diagnostics node.

Discovers protobuf service definitions at runtime via gRPC server reflection,
builds dynamic message classes, and provides a callable for making RPC requests.
Uses only ``python3-grpcio`` and ``python3-protobuf`` — no additional
dependencies.

Descriptor sets are cached at ``~/.cache/starlink_stats/`` so reflection
only runs on the first connection per firmware version.
"""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Optional

# The well-known types are registered in the default pool only once their
# _pb2 module has been imported; from_fds copies them from there.
from google.protobuf import any_pb2, api_pb2, duration_pb2  # noqa: F401
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import empty_pb2, field_mask_pb2  # noqa: F401
from google.protobuf import message_factory
from google.protobuf import source_context_pb2, struct_pb2  # noqa: F401
from google.protobuf import timestamp_pb2, type_pb2, wrappers_pb2  # noqa: F401
from google.protobuf.json_format import MessageToDict, ParseDict
import grpc


# ---- Constants ----

CACHE_DIR = Path.home() / '.cache' / 'starlink_stats'
_FDS_FILE = 'descriptors.fds'
_VERSION_FILE = 'firmware_version'

# The SpaceX device service exposed by Starlink dishes.
_DEVICE_SERVICE = 'SpaceX.API.Device.Device'
_DEVICE_METHOD = 'Handle'
_REQUEST_TYPE = 'SpaceX.API.Device.Request'
_RESPONSE_TYPE = 'SpaceX.API.Device.Response'
_RPC_PATH = f'/{_DEVICE_SERVICE}/{_DEVICE_METHOD}'

# gRPC reflection service — try v1alpha first (widely deployed), fall back
# to v1 if the dish uses the newer spec.
_REFLECTION_METHODS = [
    '/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo',
    '/grpc.reflection.v1.ServerReflection/ServerReflectionInfo',
]


# ---- Reflection protocol bootstrap ----
#
# We need ServerReflectionRequest/Response message classes to talk to the
# reflection service, but we don't have the compiled module. Build the
# descriptors programmatically from the well-known, stable proto spec.

def _build_reflection_pool() -> descriptor_pool.DescriptorPool:
    """Create a DescriptorPool containing the gRPC reflection messages."""
    pool = descriptor_pool.DescriptorPool()
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = 'grpc/reflection/v1alpha/reflection.proto'
    fdp.package = 'grpc.reflection.v1alpha'
    fdp.syntax = 'proto3'

    # --- ServerReflectionRequest ---
    req = fdp.message_type.add()
    req.name = 'ServerReflectionRequest'
    oneof = req.oneof_decl.add()
    oneof.name = 'message_request'
    _add_field(req, 'host', 1, 'TYPE_STRING')
    _add_field(req, 'file_by_filename', 3, 'TYPE_STRING', oneof_index=0)
    _add_field(req, 'file_containing_symbol', 4, 'TYPE_STRING', oneof_index=0)
    _add_field(req, 'list_services', 7, 'TYPE_STRING', oneof_index=0)

    # --- ServerReflectionResponse ---
    resp = fdp.message_type.add()
    resp.name = 'ServerReflectionResponse'
    oneof = resp.oneof_decl.add()
    oneof.name = 'message_response'
    _add_field(resp, 'valid_host', 1, 'TYPE_STRING')
    _add_field(
        resp, 'file_descriptor_response', 4, 'TYPE_MESSAGE',
        type_name='.grpc.reflection.v1alpha.FileDescriptorResponse',
        oneof_index=0,
    )
    _add_field(
        resp, 'error_response', 7, 'TYPE_MESSAGE',
        type_name='.grpc.reflection.v1alpha.ErrorResponse',
        oneof_index=0,
    )

    # --- FileDescriptorResponse ---
    fdr = fdp.message_type.add()
    fdr.name = 'FileDescriptorResponse'
    _add_field(fdr, 'file_descriptor_proto', 1, 'TYPE_BYTES', label='LABEL_REPEATED')

    # --- ErrorResponse ---
    er = fdp.message_type.add()
    er.name = 'ErrorResponse'
    _add_field(er, 'error_code', 1, 'TYPE_INT32')
    _add_field(er, 'error_message', 2, 'TYPE_STRING')

    pool.Add(fdp)
    return pool


def _add_field(
    msg: descriptor_pb2.DescriptorProto,
    name: str,
    number: int,
    field_type: str,
    *,
    label: str = 'LABEL_OPTIONAL',
    oneof_index: Optional[int] = None,
    type_name: Optional[str] = None,
) -> None:
    """Add a field to a DescriptorProto."""
    f = msg.field.add()
    f.name = name
    f.number = number
    f.type = getattr(descriptor_pb2.FieldDescriptorProto, field_type)
    f.label = getattr(descriptor_pb2.FieldDescriptorProto, label)
    if oneof_index is not None:
        f.oneof_index = oneof_index
    if type_name is not None:
        f.type_name = type_name


# Build reflection message classes once at module load.
_refl_pool = _build_reflection_pool()
_refl_factory = message_factory.MessageFactory(pool=_refl_pool)
_ReflReq = _refl_factory.GetPrototype(
    _refl_pool.FindMessageTypeByName(
        'grpc.reflection.v1alpha.ServerReflectionRequest'
    )
)
_ReflResp = _refl_factory.GetPrototype(
    _refl_pool.FindMessageTypeByName(
        'grpc.reflection.v1alpha.ServerReflectionResponse'
    )
)


# ---- Reflection client ----

def _reflection_call(channel, method, request, timeout=5.0):
    """Send one reflection request and return the response."""
    multi_callable = channel.stream_stream(
        method,
        request_serializer=type(request).SerializeToString,
        response_deserializer=_ReflResp.FromString,
    )
    response_iter = multi_callable(iter([request]), timeout=timeout)
    try:
        try:
            resp = next(response_iter)
        except StopIteration:
            raise RuntimeError('No reflection response received')
    finally:
        response_iter.cancel()
    if resp.HasField('error_response'):
        raise RuntimeError(
            f'Reflection error: {resp.error_response.error_message}'
        )
    return resp


def _fetch_descriptors_for_symbol(
    channel: grpc.Channel,
    symbol: str,
    timeout: float = 5.0,
) -> list[descriptor_pb2.FileDescriptorProto]:
    """
    Fetch all FileDescriptorProtos needed for a symbol, resolving deps.

    Recursively follows ``FileDescriptorProto.dependency`` references so
    the returned list is in dependency order (deps before dependents).
    """
    fetched: dict[str, descriptor_pb2.FileDescriptorProto] = {}

    # Try both reflection API versions.
    method = None
    last_error = None
    for candidate in _REFLECTION_METHODS:
        try:
            req = _ReflReq()
            req.file_containing_symbol = symbol
            resp = _reflection_call(channel, candidate, req, timeout)
            method = candidate
            break
        except grpc.RpcError as e:
            last_error = e
            continue
    if method is None:
        raise RuntimeError(
            f'gRPC reflection not available on server: {last_error}'
        )

    # Parse the initial response.
    for raw in resp.file_descriptor_response.file_descriptor_proto:
        fdp = descriptor_pb2.FileDescriptorProto()
        fdp.ParseFromString(raw)
        fetched[fdp.name] = fdp

    # Recursively fetch dependencies.
    to_fetch: deque[str] = deque()
    for fdp in list(fetched.values()):
        for dep in fdp.dependency:
            if dep not in fetched:
                to_fetch.append(dep)

    while to_fetch:
        dep_name = to_fetch.popleft()
        if dep_name in fetched:
            continue
        req = _ReflReq()
        req.file_by_filename = dep_name
        try:
            resp = _reflection_call(channel, method, req, timeout)
        except RuntimeError:
            # Well-known types (google/protobuf/*.proto) may not be served
            # by reflection. Skip — from_fds() uses Default() pool which
            # has them pre-loaded. For all other deps, surface the error.
            if dep_name.startswith('google/protobuf/'):
                continue
            raise
        for raw in resp.file_descriptor_response.file_descriptor_proto:
            fdp = descriptor_pb2.FileDescriptorProto()
            fdp.ParseFromString(raw)
            if fdp.name not in fetched:
                fetched[fdp.name] = fdp
                for dep in fdp.dependency:
                    if dep not in fetched:
                        to_fetch.append(dep)

    # Return in dependency order: deps before dependents.
    ordered: list[descriptor_pb2.FileDescriptorProto] = []
    added: set[str] = set()

    def _add_with_deps(name: str) -> None:
        if name in added or name not in fetched:
            return
        fdp = fetched[name]
        for dep in fdp.dependency:
            _add_with_deps(dep)
        ordered.append(fdp)
        added.add(name)

    for name in fetched:
        _add_with_deps(name)

    return ordered


def reflect(channel: grpc.Channel, timeout: float = 5.0) -> bytes:
    """
    Reflect the Starlink Device service and return a serialized FileDescriptorSet.

    The returned bytes can be cached and later passed to ``DeviceCaller.from_fds``
    to rebuild the service without another reflection call.
    """
    descriptors = _fetch_descriptors_for_symbol(channel, _DEVICE_SERVICE, timeout)
    fds = descriptor_pb2.FileDescriptorSet()
    for fdp in descriptors:
        fds.file.append(fdp)
    return fds.SerializeToString()


# ---- Device caller ----

class DeviceCaller:
    """
    Dynamic gRPC caller for the Starlink Device.Handle RPC.

    Built from reflected or cached protobuf descriptors. Instances are
    created via ``DeviceCaller.from_fds()`` and are safe to use from
    a single thread (the gRPC worker in the diagnostics node).
    """

    def __init__(self, request_class, response_class, channel):
        self._request_class = request_class
        self._response_class = response_class
        self._call = channel.unary_unary(
            _RPC_PATH,
            request_serializer=request_class.SerializeToString,
            response_deserializer=response_class.FromString,
        )

    @classmethod
    def from_fds(
        cls,
        fds_bytes: bytes,
        channel: grpc.Channel,
    ) -> 'DeviceCaller':
        """
        Build a DeviceCaller from a serialized FileDescriptorSet.

        Each call gets its own descriptor pool. The process-wide default pool
        refuses a file whose name is already loaded with different contents,
        so after a firmware update changed the dish's schema, re-reflecting
        into it raised and every later poll failed until the node restarted.
        """
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(fds_bytes)
        served = {fdp.name for fdp in fds.file}

        pool = descriptor_pool.DescriptorPool()
        # Well-known types (google/protobuf/*.proto) may not be served by
        # reflection; copy any the schema imports from the default pool,
        # where the protobuf runtime has them preloaded.
        default_pool = descriptor_pool.Default()
        copied: set[str] = set()

        def copy_with_deps(name: str) -> None:
            # Dependencies first: a pool refuses a file whose imports it
            # does not yet hold (type.proto imports any.proto, ...).
            if name in served or name in copied:
                return
            file_desc = default_pool.FindFileByName(name)
            for dep in file_desc.dependencies:
                copy_with_deps(dep.name)
            dep_proto = descriptor_pb2.FileDescriptorProto()
            file_desc.CopyToProto(dep_proto)
            pool.Add(dep_proto)
            copied.add(name)

        for fdp in fds.file:
            for dep in fdp.dependency:
                copy_with_deps(dep)
        for fdp in fds.file:
            pool.Add(fdp)

        factory = message_factory.MessageFactory(pool=pool)
        req_desc = pool.FindMessageTypeByName(_REQUEST_TYPE)
        resp_desc = pool.FindMessageTypeByName(_RESPONSE_TYPE)

        return cls(
            factory.GetPrototype(req_desc),
            factory.GetPrototype(resp_desc),
            channel,
        )

    def supports(self, request_field: str) -> bool:
        """Return True if the reflected Request schema has ``request_field``."""
        return request_field in self._request_class.DESCRIPTOR.fields_by_name

    def call(self, request_field: str, timeout: float) -> dict:
        """
        Call Device.Handle(Request(<request_field>={})) and return the full dict.

        Equivalent to ``MessageToDict(response, preserving_proto_field_name=True,
        including_default_value_fields=True)``. Raises ``KeyError`` if the
        dish's schema has no such request, so a caller can tell "this firmware
        doesn't offer it" apart from an RPC failure.
        """
        if not self.supports(request_field):
            raise KeyError(f'dish schema has no request {request_field!r}')
        request = ParseDict({request_field: {}}, self._request_class())
        response = self._call(request, timeout=timeout)
        return MessageToDict(
            response,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )

    def get_status(self, timeout: float) -> dict:
        """Call Device.Handle(Request(get_status={})) and return the full dict."""
        return self.call('get_status', timeout)

    def get_history(self, timeout: float) -> dict:
        """Call Device.Handle(Request(get_history={})) and return the full dict."""
        return self.call('get_history', timeout)


# ---- Descriptor cache ----

def load_cached_fds() -> Optional[bytes]:
    """Load cached FileDescriptorSet bytes. Returns None if not found."""
    path = CACHE_DIR / _FDS_FILE
    try:
        return path.read_bytes()
    except OSError:
        return None


def save_cached_fds(fds_bytes: bytes, firmware_version: str) -> None:
    """Save FileDescriptorSet bytes and firmware version to cache."""
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        (CACHE_DIR / _FDS_FILE).write_bytes(fds_bytes)
        (CACHE_DIR / _VERSION_FILE).write_text(firmware_version + '\n')
    except (PermissionError, OSError):
        pass  # Non-fatal — caching is best-effort.


def cached_firmware_version() -> Optional[str]:
    """Return the firmware version from the cache, or None."""
    try:
        return (CACHE_DIR / _VERSION_FILE).read_text().strip()
    except OSError:
        return None
