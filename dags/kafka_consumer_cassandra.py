from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster


class CassandraConnector:
    def __init__(self, contact_points, keyspace):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()
        self.keyspace = keyspace
        self.create_keyspace()
        self.create_table()

    def create_keyspace(self):
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} "
            "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )

    def create_table(self):
            self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.email_table (
                email text PRIMARY KEY,
                otp text
            )
        """)

    def insert_data(self, email, otp):
        self.session.execute(f"""
            INSERT INTO {self.keyspace} (email, otp)
            VALUES (%s, %s)
        """, (email, otp))

    def shutdown(self):
        self.cluster.shutdown()

    def execute_cassandra_kafka_integration(self, kafka_config, topics):
        consumer = Consumer(kafka_config)
        consumer.subscribe(topics)
        received_data = []  # Create a list to store received data

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

                    # Create a dict
                    data = {'email': email, 'otp': otp}

                    # Insert data into Cassandra table
                    self.insert_data(email, otp)
                    print(f'Received and inserted: Email={email}, OTP={otp}')

                    received_data.append(data)  # Append data to the list
        finally:
            consumer.close()  # Close the Kafka consumer

        return received_data  # Return the collected data after the loop finishes



def kafka_consumer_cassandra_main():
    # Cassandra configuration
    keyspace = 'email_namespace'
    cassandra_connector = CassandraConnector(['cassandra'], keyspace)

    # Create Cassandra keyspace and table
    cassandra_connector.create_keyspace()
    cassandra_connector.create_table()

    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
        'group.id': 'cassandra_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Kafka topics to subscribe to
    topics = ['email_topic']

    cassandra_connector.execute_cassandra_kafka_integration(kafka_config, topics)

    data = cassandra_connector.execute_cassandra_kafka_integration(kafka_config, topics)
    
    cassandra_connector.shutdown()

    return data


if __name__ == '__main__':
    kafka_consumer_cassandra_main()
