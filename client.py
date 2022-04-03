import grpc

import c_s_pb2 as pb2
import c_s_pb2_grpc as pb2_grpc

address = 'localhost'
port = 11912


class Client:

    def __init__(self):
        self.channel = grpc.insecure_channel('{}:{}'.format(address, port))
        self.stub = pb2_grpc.ChatServerStub(self.channel)

    def get_url(self, wiki_url1, wiki_url2):
        message = pb2.request(url1=wiki_url1, url2=wiki_url2)
        #print(f'{message}')
        return self.stub.SendRequest(message)


if __name__ == '__main__':
    client = Client()
    url1 = input("URL1:")
    url2 = input("URL2:")
    result = client.get_url(url1, url2)
    print(result.ans)
