import pika
import os

class myBroker:
    def __init__(self,connection) -> None:
        Connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = Connection.channel()
        topicList = []
        #DECLARING QUEUE
        channel.queue_declare(queue='hello')
        print("myBroker Initialised")


    def createTopic(self,topicname):
        os.mkdir('/brokerFS/,mode = 0o777, *, dir_fd = None')
        pass

    def removeTopic(self,topicname):
        pass

class myKafkaProducer(myBroker):
    def __init__(self) -> None:
        print("myKafkaProducer Initialised")
        pass
    
    def produce(topic):
        if topic not in myBroker.topicsList:
            myBroker.createTopic(topic)
        myBroker.channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='Hello World!')
        print(" [x] Sent 'Hello World!'")

class myKafkaConsumer(myBroker):
    def __init__(self,server,topic) -> None:
        pass

    # def consume(topic):
    #     def callback(ch, method, properties, body):
    #         print(" [x] Received %r" % body)

    #     channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    #     print(' [*] Waiting for messages. To exit press CTRL+C')
    #     channel.start_consuming()

