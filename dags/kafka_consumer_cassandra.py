from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from cassandra.cluster import Cluster

class CassandraConnector:
    def __init__(self, contact_points):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()
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
            INSERT INTO email_namespace.email_table (email, otp)
            VALUES (%s, %s)
        """, (email, otp))

    def shutdown(self):
        self.cluster.shutdown()


def fetch_and_insert_messages(kafka_config, cassandra_connector, topic):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

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
                cassandra_connector.insert_data(email, otp)
                print(f'Received and inserted: Email={email}, OTP={otp}')
    except KeyboardInterrupt:
        print("Received KeyboardInterrupt. Closing consumer.")
    finally:
        consumer.close()

def kafka_consumer_cassandra_main():
    # Cassandra configuration
    cassandra_connector = CassandraConnector(['cassandra'])

    # Create Cassandra keyspace and table
    cassandra_connector.create_keyspace()
    cassandra_connector.create_table()

    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'cassandra_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Kafka topic to subscribe to
    topic = 'email_topic'

    # Fetch and insert existing messages
    fetch_and_insert_messages(kafka_config, cassandra_connector, topic)

    cassandra_connector.shutdown()

if __name__ == '__main__':
    kafka_consumer_cassandra_main()