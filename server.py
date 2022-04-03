import json
import threading
import uuid
from collections import deque
from concurrent import futures

import grpc
import pika

import c_s_pb2 as pb2
import c_s_pb2_grpc as pb2_grpc


class RpcClient(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, url):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=url.encode("utf-8"))
        while self.response is None:
            self.connection.process_data_events()
        return self.response


class ChatServer(pb2_grpc.ChatServerServicer):

    def __init__(self):
        self.finish = False
        self.res = None

    def SendRequest(self, request: pb2.request, context):
        """
        This method is called when a clients sends a Note to the server.
        :param request:
        :param context:
        :return:
        """
        was = []
        path = {request.url1: ["URL1"]}
        queue = deque()
        queue.append(request.url1)
        was.append(request.url1)
        while len(queue) > 0:
            rpc = RpcClient()
            head = queue.popleft()
            response = rpc.call(head)
            ans = json.loads(response.decode())
            parent = ans[0]
            print(len(ans))
            ans = ans[1:]
            names = [x[0] for x in ans]
            links = [x[1] for x in ans]
            if request.url2 in links:
                p = path[parent[1]].copy()
                p.append("URL2")
                res = pb2.response()
                res.ans = ' => '.join(p)
                print(f'{res}')
                return res
            else:
                for i in range(len(links)):
                    if links[i] not in was:
                        was.append(links[i])
                        p = path[parent[1]].copy()
                        p.append(names[i])
                        path[links[i]] = p
                        queue.append(links[i])

if __name__ == '__main__':
    port = 11912
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # create a gRPC server
    pb2_grpc.add_ChatServerServicer_to_server(ChatServer(), server)  # register the server to gRPC
    print('Starting server. Listening...')
    server.add_insecure_port('[::]:' + str(port))
    server.start()
    server.wait_for_termination()
