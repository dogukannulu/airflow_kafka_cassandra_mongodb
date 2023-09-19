from confluent_kafka import Producer

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        """
        Initializes the Kafka producer with the given bootstrap servers.
        """
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, value):
        """
        Produces a message to the specified Kafka topic with the given key and value.
        """
        self.producer.produce(topic, key=key, value=value)
        self.producer.flush()

def kafka_producer_main():
    bootstrap_servers = 'kafka1:19092,kafka2:19093,kafka3:19094'
    kafka_producer = KafkaProducerWrapper(bootstrap_servers)
    
    topic = "email_topic"
    key = "sample_email@my_email.com"
    value = "1234567"
    
    kafka_producer.produce_message(topic, key, value)

if __name__ == "__main__":
    kafka_producer_main()
