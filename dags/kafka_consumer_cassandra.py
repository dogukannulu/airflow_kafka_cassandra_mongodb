from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster

class CassandraConnector:
    def __init__(self, contact_points, keyspace):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)
        self.create_keyspace()
        self.create_table()

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS email_namespace
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)

    def create_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS email_namespace.email_table (
                email text PRIMARY KEY,
                otp text
            )
        """)

    def insert_data(self, email, otp):
        self.session.execute("""
            INSERT INTO email_table (email, otp)
            VALUES (%s, %s)
        """, (email, otp))

    def shutdown(self):
        self.cluster.shutdown()

    def execute_cassandra_kafka_integration(self, kafka_config, topics):
        consumer = Consumer(kafka_config)
        consumer.subscribe(topics)

        try:
            while True:
                msg = consumer.poll(1.0)

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

                    # Insert data into Cassandra table
                    self.insert_data(email, otp)
                    print(f'Received and inserted: Email={email}, OTP={otp}')
        finally:
            consumer.close()
            self.shutdown()


# Cassandra configuration
cassandra_connector = CassandraConnector(['cassandra'], 'email_namespace')

def cassandra_main():
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094', 
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Kafka topics to subscribe to
    topics = ['email_topic']

    # Create a Kafka consumer and consume messages
    kafka_consumer = KafkaConsumerWrapperCassandra(kafka_config, topics)
    try:
        kafka_consumer.consume_messages()
    finally:
        kafka_consumer.close()
        cassandra_connector.shutdown()
