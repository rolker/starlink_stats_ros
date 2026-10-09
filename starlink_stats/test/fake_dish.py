# Copyright 2026 University of New Hampshire
#
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file or at
# https://developers.google.com/open-source/licenses/bsd

"""
A fake Starlink dish for tests: a local gRPC server with reflection.

The schema is a small stand-in for the dish's own. Field names follow what
the dish reported through reflection on firmware 2026.08.10 as transcribed by
github.com/AndyMcLeod/starlink-monitor; field numbers here are arbitrary,
since the node only ever learns them through reflection. It is NOT the real
schema: replace it with a FileDescriptorSet captured from a dish (the node
caches one under ~/.cache/starlink_stats/) when one is available.

Like the Mini on that firmware, the status has no ``state`` field.
"""

from __future__ import annotations

from concurrent import futures

from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory
from google.protobuf.json_format import ParseDict
import grpc

from starlink_stats.grpc_reflection import _ReflReq, _ReflResp

_F = descriptor_pb2.FieldDescriptorProto
_PKG = 'SpaceX.API.Device'
FILE_NAME = 'spacex/api/device/device.proto'


def _msg(parent, name, fields):
    """Add a message ``name`` with ``fields`` = [(name, type, label, type_name)]."""
    m = parent.message_type.add() if hasattr(parent, 'message_type') else \
        parent.nested_type.add()
    m.name = name
    for number, (fname, ftype, label, type_name) in enumerate(fields, start=1):
        f = m.field.add()
        f.name = fname
        f.number = number
        f.type = ftype
        f.label = label
        if type_name:
            f.type_name = type_name
    return m


def build_fds(with_history: bool = True, extra_status_field: str = '') -> bytes:
    """
    Build the fake dish's FileDescriptorSet.

    ``with_history=False`` leaves ``get_history`` out of the Request, as an
    older or different firmware might. ``extra_status_field`` adds a float
    field to the status message, to model a firmware update that changes the
    schema under the same file name.
    """
    opt, rep = _F.LABEL_OPTIONAL, _F.LABEL_REPEATED
    flt, b, u32, u64, i32, s, msg = (
        _F.TYPE_FLOAT, _F.TYPE_BOOL, _F.TYPE_UINT32, _F.TYPE_UINT64,
        _F.TYPE_INT32, _F.TYPE_STRING, _F.TYPE_MESSAGE,
    )
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = FILE_NAME
    fdp.package = _PKG
    fdp.syntax = 'proto3'

    def t(name):
        return f'.{_PKG}.{name}'

    _msg(fdp, 'DeviceInfo', [
        ('id', s, opt, None), ('hardware_version', s, opt, None),
        ('software_version', s, opt, None), ('country_code', s, opt, None),
    ])
    _msg(fdp, 'DeviceState', [('uptime_s', u64, opt, None)])
    _msg(fdp, 'DishObstructionStats', [
        ('fraction_obstructed', flt, opt, None), ('valid_s', flt, opt, None),
        ('currently_obstructed', b, opt, None), ('time_obstructed', flt, opt, None),
        ('avg_prolonged_obstruction_duration_s', flt, opt, None),
    ])
    _msg(fdp, 'DishGpsStats', [
        ('gps_valid', b, opt, None), ('gps_sats', u32, opt, None),
        ('inhibit_gps', b, opt, None),
    ])
    _msg(fdp, 'AlignmentStats', [
        ('tilt_angle_deg', flt, opt, None),
        ('boresight_azimuth_deg', flt, opt, None),
        ('boresight_elevation_deg', flt, opt, None),
        ('attitude_uncertainty_deg', flt, opt, None),
        ('desired_boresight_azimuth_deg', flt, opt, None),
        ('desired_boresight_elevation_deg', flt, opt, None),
    ])
    _msg(fdp, 'DishReadyStates', [
        (n, b, opt, None) for n in ('cady', 'scp', 'l1l2', 'xphy', 'aap', 'rf')
    ])
    status_fields = [
        ('device_info', msg, opt, t('DeviceInfo')),
        ('device_state', msg, opt, t('DeviceState')),
        ('pop_ping_drop_rate', flt, opt, None),
        ('pop_ping_latency_ms', flt, opt, None),
        ('downlink_throughput_bps', flt, opt, None),
        ('uplink_throughput_bps', flt, opt, None),
        ('obstruction_stats', msg, opt, t('DishObstructionStats')),
        ('gps_stats', msg, opt, t('DishGpsStats')),
        ('alignment_stats', msg, opt, t('AlignmentStats')),
        ('ready_states', msg, opt, t('DishReadyStates')),
        ('is_snr_above_noise_floor', b, opt, None),
        ('eth_speed_mbps', i32, opt, None),
    ]
    if extra_status_field:
        status_fields.append((extra_status_field, flt, opt, None))
    _msg(fdp, 'DishGetStatusResponse', status_fields)
    _msg(fdp, 'DishGetHistoryResponse', [
        ('current', u64, opt, None),
        ('pop_ping_drop_rate', flt, rep, None),
        ('pop_ping_latency_ms', flt, rep, None),
        ('downlink_throughput_bps', flt, rep, None),
        ('uplink_throughput_bps', flt, rep, None),
        ('power_in', flt, rep, None),
    ])
    _msg(fdp, 'GetStatusRequest', [])
    _msg(fdp, 'GetHistoryRequest', [])

    req_fields = [('get_status', msg, opt, t('GetStatusRequest'))]
    resp_fields = [('dish_get_status', msg, opt, t('DishGetStatusResponse'))]
    if with_history:
        req_fields.append(('get_history', msg, opt, t('GetHistoryRequest')))
        resp_fields.append(('dish_get_history', msg, opt, t('DishGetHistoryResponse')))
    for name, fields, oneof in (('Request', req_fields, 'request'),
                                ('Response', resp_fields, 'response')):
        m = _msg(fdp, name, fields)
        m.oneof_decl.add().name = oneof
        for f in m.field:
            f.oneof_index = 0

    svc = fdp.service.add()
    svc.name = 'Device'
    method = svc.method.add()
    method.name = 'Handle'
    method.input_type = t('Request')
    method.output_type = t('Response')

    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fdp)
    return fds.SerializeToString()


def default_status() -> dict:
    """Return a plausible connected-Mini status, as the dish would encode it."""
    return {
        'device_info': {
            'id': 'ut-fake-0001', 'hardware_version': 'mini1_prod2',
            'software_version': '2026.08.10.cr84226.52049', 'country_code': 'US',
        },
        'device_state': {'uptime_s': '19192'},
        'pop_ping_drop_rate': 0.0,
        'pop_ping_latency_ms': 22.0,
        'downlink_throughput_bps': 20728.25,
        'uplink_throughput_bps': 107181.24,
        'obstruction_stats': {'fraction_obstructed': 0.001, 'valid_s': 18861.0},
        'gps_stats': {'gps_valid': True, 'gps_sats': 14},
        'alignment_stats': {
            'tilt_angle_deg': 3.5, 'attitude_uncertainty_deg': 0.4,
            'boresight_azimuth_deg': 178.0, 'boresight_elevation_deg': 76.0,
            'desired_boresight_azimuth_deg': 179.0,
            'desired_boresight_elevation_deg': 75.0,
        },
        'ready_states': {
            'cady': True, 'scp': True, 'l1l2': True,
            'xphy': True, 'aap': True, 'rf': True,
        },
        'is_snr_above_noise_floor': True,
        'eth_speed_mbps': 1000,
    }


def default_history(samples: int = 900, current: int = 1234) -> dict:
    """Return a ring buffer whose newest sample, index (current-1) % samples, is distinct."""
    power = [50.0] * samples
    power[(current - 1) % samples] = 77.5
    return {
        'current': str(current),
        'pop_ping_drop_rate': [0.0] * samples,
        'pop_ping_latency_ms': [20.0] * samples,
        'downlink_throughput_bps': [1000.0] * samples,
        'uplink_throughput_bps': [500.0] * samples,
        'power_in': power,
    }


class FakeDish:
    """
    Serve the fake schema on 127.0.0.1 with reflection and Device.Handle.

    ``status`` and ``history`` are dicts the test can replace at any time;
    ``fail`` makes every Handle call abort with UNAVAILABLE, ``fail_history``
    only the history calls. ``requests`` counts calls by request kind.
    """

    def __init__(self, fds_bytes: bytes | None = None):
        self.set_schema(fds_bytes or build_fds())
        self.status = default_status()
        self.history = default_history()
        self.fail = False
        self.fail_history = False
        self.requests: dict[str, int] = {}
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self._server.add_generic_rpc_handlers((self._handler(),))
        self.port = self._server.add_insecure_port('127.0.0.1:0')
        self.address = f'127.0.0.1:{self.port}'

    def set_schema(self, fds_bytes: bytes) -> None:
        """Swap the served schema, as a firmware update would."""
        self.fds_bytes = fds_bytes
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(fds_bytes)
        self._files = {f.name: f.SerializeToString() for f in fds.file}
        pool = descriptor_pool.DescriptorPool()
        for f in fds.file:
            pool.Add(f)
        factory = message_factory.MessageFactory(pool=pool)
        self._req_cls = factory.GetPrototype(
            pool.FindMessageTypeByName(f'{_PKG}.Request'))
        self._resp_cls = factory.GetPrototype(
            pool.FindMessageTypeByName(f'{_PKG}.Response'))

    def start(self) -> 'FakeDish':
        self._server.start()
        return self

    def stop(self) -> None:
        self._server.stop(grace=None)

    # --- handlers ---

    def _handle(self, request_bytes, context):
        request = self._req_cls.FromString(request_bytes)
        kind = request.WhichOneof('request')
        self.requests[kind] = self.requests.get(kind, 0) + 1
        if self.fail or (self.fail_history and kind == 'get_history'):
            context.abort(grpc.StatusCode.UNAVAILABLE, 'fake dish offline')
        if kind == 'get_status':
            body = {'dish_get_status': self.status}
        elif kind == 'get_history':
            body = {'dish_get_history': self.history}
        else:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, f'no {kind}')
        return ParseDict(body, self._resp_cls()).SerializeToString()

    def _reflect(self, request_iterator, context):
        for raw in request_iterator:
            req = _ReflReq.FromString(raw)
            resp = _ReflResp()
            which = req.WhichOneof('message_request')
            if which == 'file_containing_symbol':
                resp.file_descriptor_response.file_descriptor_proto.extend(
                    self._files.values())
            elif which == 'file_by_filename' and req.file_by_filename in self._files:
                resp.file_descriptor_response.file_descriptor_proto.append(
                    self._files[req.file_by_filename])
            else:
                resp.error_response.error_code = grpc.StatusCode.NOT_FOUND.value[0]
                resp.error_response.error_message = f'not found: {which}'
            yield resp.SerializeToString()

    def _handler(self):
        dish = self

        class _Handler(grpc.GenericRpcHandler):

            def service(self, call_details):
                if call_details.method == '/SpaceX.API.Device.Device/Handle':
                    return grpc.unary_unary_rpc_method_handler(dish._handle)
                if call_details.method == (
                        '/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo'):
                    return grpc.stream_stream_rpc_method_handler(dish._reflect)
                return None

        return _Handler()
