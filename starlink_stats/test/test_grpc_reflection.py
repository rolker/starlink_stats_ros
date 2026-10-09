# Copyright 2024 Avery Munoz
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""Unit tests for the gRPC reflection client bootstrap and caching."""

from google.protobuf import descriptor_pb2

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
        assert parsed.WhichOneof('message_request') == 'list_services'

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

    def test_from_fds_builds_twice(self):
        """Building from the same descriptors twice succeeds."""
        import grpc
        channel = grpc.insecure_channel('localhost:1')
        fds_bytes = self._make_minimal_fds()
        caller1 = DeviceCaller.from_fds(fds_bytes, channel)
        caller2 = DeviceCaller.from_fds(fds_bytes, channel)
        assert caller1 is not None
        assert caller2 is not None
        channel.close()

    def test_from_fds_accepts_a_changed_schema_under_the_same_name(self):
        """
        A firmware update can change the schema without renaming its files.

        Regression: descriptors used to go into the process-wide default pool,
        which raised on the second, different file of the same name.
        """
        import grpc
        from fake_dish import build_fds
        channel = grpc.insecure_channel('localhost:1')
        before = DeviceCaller.from_fds(build_fds(), channel)
        after = DeviceCaller.from_fds(
            build_fds(extra_status_field='new_firmware_field'), channel)
        status_before = before._response_class.DESCRIPTOR.fields_by_name[
            'dish_get_status'].message_type
        status_after = after._response_class.DESCRIPTOR.fields_by_name[
            'dish_get_status'].message_type
        assert 'new_firmware_field' not in status_before.fields_by_name
        assert 'new_firmware_field' in status_after.fields_by_name
        channel.close()

    def test_from_fds_copies_well_known_imports(self):
        """A schema importing a well-known type that reflection did not serve builds."""
        import grpc
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(self._make_minimal_fds())
        device = fds.file[1]
        device.dependency.append('google/protobuf/timestamp.proto')
        f = device.message_type[1].nested_type[0].field.add()
        f.name = 'when'
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = '.google.protobuf.Timestamp'
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        channel = grpc.insecure_channel('localhost:1')
        caller = DeviceCaller.from_fds(fds.SerializeToString(), channel)
        assert caller is not None
        channel.close()

    def test_from_fds_copies_well_known_imports_with_their_own_imports(self):
        """type.proto imports any.proto and source_context.proto; all must be copied."""
        import grpc
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(self._make_minimal_fds())
        device = fds.file[1]
        device.dependency.append('google/protobuf/type.proto')
        f = device.message_type[1].nested_type[0].field.add()
        f.name = 'kind'
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = '.google.protobuf.Type'
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        channel = grpc.insecure_channel('localhost:1')
        caller = DeviceCaller.from_fds(fds.SerializeToString(), channel)
        assert caller is not None
        channel.close()

    def test_from_fds_mixes_served_and_copied_well_known_files(self):
        """
        The dish serves any.proto (listed last) but not type.proto, which imports it.

        Every file must still land in the pool after its imports.
        """
        import grpc
        from google.protobuf import any_pb2
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(self._make_minimal_fds())
        device = fds.file[1]
        device.dependency.append('google/protobuf/type.proto')
        f = device.message_type[1].nested_type[0].field.add()
        f.name = 'kind'
        f.number = 1
        f.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        f.type_name = '.google.protobuf.Type'
        f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        any_proto = fds.file.add()
        any_pb2.DESCRIPTOR.CopyToProto(any_proto)
        channel = grpc.insecure_channel('localhost:1')
        caller = DeviceCaller.from_fds(fds.SerializeToString(), channel)
        assert caller is not None
        channel.close()

    def test_supports_reports_missing_requests(self):
        """supports()/call() tell 'firmware has no such request' from an RPC failure."""
        import grpc
        import pytest
        from fake_dish import build_fds
        channel = grpc.insecure_channel('localhost:1')
        with_hist = DeviceCaller.from_fds(build_fds(), channel)
        without = DeviceCaller.from_fds(build_fds(with_history=False), channel)
        assert with_hist.supports('get_history')
        assert not without.supports('get_history')
        with pytest.raises(KeyError):
            without.get_history(timeout=0.1)
        channel.close()


# --- DeviceCaller against a running fake dish ---

class TestDeviceCallerLive:
    """Reflect and call a local fake dish over real gRPC."""

    def test_reflect_status_and_history(self):
        import grpc
        from fake_dish import FakeDish
        from starlink_stats.grpc_reflection import reflect
        dish = FakeDish().start()
        try:
            channel = grpc.insecure_channel(dish.address)
            caller = DeviceCaller.from_fds(reflect(channel, timeout=5.0), channel)
            status = caller.get_status(timeout=5.0)['dish_get_status']
            assert status['device_info']['hardware_version'] == 'mini1_prod2'
            assert status['gps_stats']['gps_valid'] is True
            assert 'state' not in status
            history = caller.get_history(timeout=5.0)['dish_get_history']
            assert history['current'] == '1234'
            assert len(history['power_in']) == 900
            assert dish.requests == {'get_status': 1, 'get_history': 1}
            channel.close()
        finally:
            dish.stop()
