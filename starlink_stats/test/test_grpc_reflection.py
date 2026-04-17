# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Unit tests for the gRPC reflection client bootstrap and caching."""

from google.protobuf import descriptor_pb2
import pytest

from starlink_stats.grpc_reflection import (
    _build_reflection_pool,
    _ReflReq,
    _ReflResp,
    cached_firmware_version,
    DeviceCaller,
    load_cached_fds,
    save_cached_fds,
)


# --- Reflection message bootstrap ---

class TestReflectionBootstrap:
    """Verify the programmatically-built reflection proto messages work."""

    def test_request_class_exists(self):
        """Request class was built from the reflection pool."""
        assert _ReflReq is not None
        assert _ReflReq.DESCRIPTOR.full_name == (
            'grpc.reflection.v1alpha.ServerReflectionRequest'
        )

    def test_response_class_exists(self):
        """Response class was built from the reflection pool."""
        assert _ReflResp is not None
        assert _ReflResp.DESCRIPTOR.full_name == (
            'grpc.reflection.v1alpha.ServerReflectionResponse'
        )

    def test_request_file_containing_symbol(self):
        """Can set the file_containing_symbol oneof field."""
        req = _ReflReq()
        req.file_containing_symbol = 'SpaceX.API.Device.Device'
        data = req.SerializeToString()
        assert len(data) > 0
        parsed = _ReflReq()
        parsed.ParseFromString(data)
        assert parsed.file_containing_symbol == 'SpaceX.API.Device.Device'

    def test_request_list_services(self):
        """Can set the list_services oneof field."""
        req = _ReflReq()
        req.list_services = ''
        data = req.SerializeToString()
        parsed = _ReflReq()
        parsed.ParseFromString(data)
        assert parsed.HasField('message_request')

    def test_request_file_by_filename(self):
        """Can set the file_by_filename oneof field."""
        req = _ReflReq()
        req.file_by_filename = 'spacex/api/device/device.proto'
        data = req.SerializeToString()
        parsed = _ReflReq()
        parsed.ParseFromString(data)
        assert parsed.file_by_filename == 'spacex/api/device/device.proto'

    def test_response_roundtrip(self):
        """Response can hold a FileDescriptorResponse with raw bytes."""
        resp = _ReflResp()
        fdr = resp.file_descriptor_response
        # Simulate adding a FileDescriptorProto as raw bytes.
        fdp = descriptor_pb2.FileDescriptorProto()
        fdp.name = 'test.proto'
        fdp.package = 'test'
        fdr.file_descriptor_proto.append(fdp.SerializeToString())
        data = resp.SerializeToString()
        assert len(data) > 0
        parsed = _ReflResp()
        parsed.ParseFromString(data)
        assert parsed.HasField('file_descriptor_response')
        assert len(parsed.file_descriptor_response.file_descriptor_proto) == 1

    def test_pool_has_all_messages(self):
        """Pool contains all four reflection message types."""
        pool = _build_reflection_pool()
        for name in [
            'grpc.reflection.v1alpha.ServerReflectionRequest',
            'grpc.reflection.v1alpha.ServerReflectionResponse',
            'grpc.reflection.v1alpha.FileDescriptorResponse',
            'grpc.reflection.v1alpha.ErrorResponse',
        ]:
            desc = pool.FindMessageTypeByName(name)
            assert desc is not None, f'{name} not in pool'


# --- Descriptor caching ---

class TestDescriptorCache:
    """Test load/save of cached FileDescriptorSet bytes."""

    def test_roundtrip(self, tmp_path, monkeypatch):
        """Save and load produces identical bytes."""
        monkeypatch.setattr(
            'starlink_stats.grpc_reflection.CACHE_DIR', tmp_path,
        )
        fds = descriptor_pb2.FileDescriptorSet()
        fdp = fds.file.add()
        fdp.name = 'test.proto'
        fds_bytes = fds.SerializeToString()

        save_cached_fds(fds_bytes, '2026.04.01.mr77223')
        loaded = load_cached_fds()
        assert loaded == fds_bytes

    def test_firmware_version_stored(self, tmp_path, monkeypatch):
        """Firmware version is persisted alongside descriptors."""
        monkeypatch.setattr(
            'starlink_stats.grpc_reflection.CACHE_DIR', tmp_path,
        )
        save_cached_fds(b'data', '2026.04.01.mr77223')
        assert cached_firmware_version() == '2026.04.01.mr77223'

    def test_load_missing_returns_none(self, tmp_path, monkeypatch):
        """Missing cache file returns None, not an exception."""
        monkeypatch.setattr(
            'starlink_stats.grpc_reflection.CACHE_DIR', tmp_path,
        )
        assert load_cached_fds() is None
        assert cached_firmware_version() is None


# --- DeviceCaller.from_fds ---

class TestDeviceCallerFromFds:
    """Test building a DeviceCaller from a synthetic FileDescriptorSet."""

    @staticmethod
    def _make_minimal_fds() -> bytes:
        """Build a minimal FileDescriptorSet for SpaceX.API.Device."""
        fds = descriptor_pb2.FileDescriptorSet()

        # File 1: common types (empty, just satisfies the import)
        common = fds.file.add()
        common.name = 'spacex/api/common/status/status.proto'
        common.package = 'SpaceX.API.Status'
        common.syntax = 'proto3'

        # File 2: device.proto with Request, Response, and Device service
        device = fds.file.add()
        device.name = 'spacex/api/device/device.proto'
        device.package = 'SpaceX.API.Device'
        device.syntax = 'proto3'

        # Request message with get_status oneof
        req = device.message_type.add()
        req.name = 'Request'
        oneof = req.oneof_decl.add()
        oneof.name = 'request'
        # GetStatusRequest (empty nested message)
        gs = req.nested_type.add()
        gs.name = 'GetStatusRequest'
        f = req.field.add()
        f.name = 'get_status'
        f.number = 1006
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = '.SpaceX.API.Device.Request.GetStatusRequest'
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        f.oneof_index = 0

        # Response message with dish_get_status oneof
        resp = device.message_type.add()
        resp.name = 'Response'
        oneof = resp.oneof_decl.add()
        oneof.name = 'response'
        # DishGetStatusResponse (empty nested message)
        dgs = resp.nested_type.add()
        dgs.name = 'DishGetStatusResponse'
        f = resp.field.add()
        f.name = 'dish_get_status'
        f.number = 1005
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = '.SpaceX.API.Device.Response.DishGetStatusResponse'
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        f.oneof_index = 0

        # Device service with Handle method
        svc = device.service.add()
        svc.name = 'Device'
        method = svc.method.add()
        method.name = 'Handle'
        method.input_type = '.SpaceX.API.Device.Request'
        method.output_type = '.SpaceX.API.Device.Response'

        return fds.SerializeToString()

    def test_builds_from_synthetic_fds(self):
        """Build a DeviceCaller from a synthetic descriptor set."""
        import grpc
        # Use a dummy channel — we won't actually call the RPC.
        channel = grpc.insecure_channel('localhost:1')
        fds_bytes = self._make_minimal_fds()
        caller = DeviceCaller.from_fds(fds_bytes, channel)
        assert caller is not None
        assert caller._request_class is not None
        assert caller._response_class is not None
        channel.close()

    def test_rejects_empty_fds(self):
        """Raise on empty FileDescriptorSet — message type is missing."""
        import grpc
        channel = grpc.insecure_channel('localhost:1')
        with pytest.raises(KeyError):
            DeviceCaller.from_fds(b'', channel)
        channel.close()
