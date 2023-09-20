from confluent_kafka.admin import AdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

admin_config = {
    'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
    'client.id': 'kafka_admin_client'
}

admin_client = AdminClient(admin_config)

def kafka_create_topic_main():
    """Checks if the topic email_topic exists or not. If not, creates the topic."""
    topic_name = 'email_topic'
    
    try:
        # Create the new topic
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
        admin_client.create_topics([new_topic])
        return "topic_created"
    except:
        return "topic_already_exists"


if __name__ == "__main__":
    result = kafka_create_topic_main()
    print(result)
