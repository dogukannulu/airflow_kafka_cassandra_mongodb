from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

class MongoDBConnector:
    def __init__(self, mongodb_uri, database_name, collection_name):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def insert_data(self, email, otp):
        document = {
            "email": email,
            "otp": otp
        }
        self.collection.insert_one(document)

    def close(self):
        self.client.close()

class KafkaConsumerWrapperMongoDB:
    def __init__(self, kafka_config, topics):
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe(topics)

    @classmethod
    def consume_messages(self):
        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print('Error: {}'.format(msg.error()))
            else:
                email = msg.key().decode('utf-8')
                otp = msg.value().decode('utf-8')

                # Create a dict
                data = {'email': email, 'otp': otp}

                # Insert data into MongoDB collection
                mongodb_connector.insert_data(email, otp)
                print(f'Received and inserted: Email={email}, OTP={otp}')

                return data

    def close(self):
        self.consumer.close()

# MongoDB configuration
mongodb_connector = MongoDBConnector('mongodb://root:root@mongo:27017/', 'email_namespace', 'email_table')


def mongodb_main():
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094', 
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Kafka topics to subscribe to
    topics = ['email_topic']

    # Create a Kafka consumer and consume messages
    kafka_consumer = KafkaConsumerWrapperMongoDB(kafka_config, topics)
    try:
        kafka_consumer.consume_messages()
    finally:
        kafka_consumer.close()
        mongodb_connector.close()
