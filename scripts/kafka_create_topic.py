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

    existing_topics = admin_client.list_topics().topics
    if topic_name in existing_topics:
        return "Exists"
    
    # Create the new topic
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
    admin_client.create_topics([new_topic])
    return "Created"


if __name__ == "__main__":
    result = kafka_create_topic_main()
    logger.info(result)