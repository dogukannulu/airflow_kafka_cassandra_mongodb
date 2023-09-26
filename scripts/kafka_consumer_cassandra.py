from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from cassandra.cluster import Cluster
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

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


def fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    start_time = time.time()
    try:
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= run_duration_secs:
                break
            
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('Reached end of partition')
                else:
                    logger.error('Error: {}'.format(msg.error()))
            else:
                email = msg.key().decode('utf-8')
                otp = msg.value().decode('utf-8')


                query = "SELECT email FROM email_namespace.email_table WHERE email = %s"
                existing_email = cassandra_connector.session.execute(query, (email,)).one()

                if existing_email:
                    logger.warning(f'Skipped existing email: Email={email}')
                else:
                    cassandra_connector.insert_data(email, otp)
                    logger.info(f'Received and inserted: Email={email}, OTP={otp}')
                            
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Closing consumer.")
    finally:
        consumer.close()


def kafka_consumer_cassandra_main():
    cassandra_connector = CassandraConnector(['cassandra'])

    cassandra_connector.create_keyspace()
    cassandra_connector.create_table()

    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'cassandra_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topic = 'email_topic'
    run_duration_secs = 30

    fetch_and_insert_messages(kafka_config, cassandra_connector, topic, run_duration_secs)

    cassandra_connector.shutdown()

if __name__ == '__main__':
    kafka_consumer_cassandra_main()
