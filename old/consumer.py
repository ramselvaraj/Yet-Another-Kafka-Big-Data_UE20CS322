from kafka import myKafkaConsumer

consumer = myKafkaConsumer('topic1','broker1')

data = consumer.consume()
