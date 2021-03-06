# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import c_s_pb2 as c__s__pb2


class ChatServerStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SendRequest = channel.unary_unary(
                '/grpc.ChatServer/SendRequest',
                request_serializer=c__s__pb2.request.SerializeToString,
                response_deserializer=c__s__pb2.response.FromString,
                )


class ChatServerServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SendRequest(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ChatServerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SendRequest': grpc.unary_unary_rpc_method_handler(
                    servicer.SendRequest,
                    request_deserializer=c__s__pb2.request.FromString,
                    response_serializer=c__s__pb2.response.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'grpc.ChatServer', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class ChatServer(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SendRequest(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/grpc.ChatServer/SendRequest',
            c__s__pb2.request.SerializeToString,
            c__s__pb2.response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
