from kafka import myKafkaProducer
#from kafka import myBroker

producer = myKafkaProducer()
data = input("Enter Producer Message: ")
producer.produce('topic1',data)

