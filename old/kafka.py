import pika
import os

class myBroker:
    def __init__(self,queueName) -> None:
        self.queueName = queueName
        self.Connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.Connection.channel()
        #DECLARING QUEUE
        self.channel.queue_declare(queue=('{}').format(queueName))
        #print("myBroker Initialised")


    def createTopic(self,topicname):
        global topicList
        topicList.append(topicname)
        pass

    def removeTopic(self,topicname):
        pass

class myKafkaProducer():
    def __init__(self) -> None:
        print("myKafkaProducer Initialised")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        #DECLARING QUEUE
        self.channel.queue_declare(queue='hello')
        pass
    #def produceBroker(self,topic, value, broker):
    
    def produce(self,topic,value):
        #self.produceBroker(topic, value,broker1)
        self.channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=value)
        print(" [x] Sent 'Hello World!'")
        self.connection.close()

class myKafkaConsumer():
    def __init__(self,server,topic) -> None:
        self.topic = topic
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='hello')
        pass
    #def consumeBroker(self, broker):
        
    def consume(self):
        #self.consumeBroker(broker1)
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)

        self.channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()




