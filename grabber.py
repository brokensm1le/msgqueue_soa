import json

import pika
import requests
from bs4 import BeautifulSoup


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def grab(url):
    res = []
    res.append(("Parent", url))
    html = requests.get(url).text
    bs4 = BeautifulSoup(html, "html.parser")
    links = bs4.find_all('a')
    for link in links:
        try:
            if link['title'] == None or link['href'] == None:
                continue
            if link["href"][:2] == '/w':
                res.append((link['title'], "https://ru.wikipedia.org" + link["href"]))
            elif link["href"][:5] == 'https':
                res.append((link['title'], link['href']))
        except KeyError:
            pass
    return json.dumps(res).encode("utf-8")


def on_request(ch, method, props, body):
    url = body.decode("utf-8")

    print(" [.] request", url)

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=grab(url))
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()

# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body.decode())
#     res = []
#     url = body.decode()
#     res.append(("Parent", body.decode()))
#     html = requests.get(url).text
#     bs4 = BeautifulSoup(html, "html.parser")
#     links = bs4.find_all('a')
#     for link in links:
#         try:
#             if link['title'] == None or link['href'] == None:
#                 continue
#             if link["href"][:2] == '/w':
#                 res.append((link['title'], "https://ru.wikipedia.org" + link["href"]))
#             elif link["href"][:5] == 'https':
#                 res.append((link['title'], link['href']))
#         except KeyError:
#             pass
#
#     queue.append(json.dumps(res).encode("utf-8"))
#
#     time.sleep(body.count(b'.'))
#     print(" [x] Done")
#
#     ch.basic_ack(delivery_tag=method.delivery_tag)
#
#
# channel.basic_qos(prefetch_count=1)
# channel.basic_consume(queue='task_queue', on_message_callback=callback)
#
# channel.start_consuming()
