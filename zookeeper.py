import pika


Connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = Connection.channel()
topicList = []
#DECLARING QUEUE
channel.queue_declare(queue='hello')
print("myBroker Initialised")