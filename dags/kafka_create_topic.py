from confluent_kafka.admin import AdminClient, NewTopic

admin_config = {
    'bootstrap.servers': 'kafka1:19092,kafka2:19093,kafka3:19094',
    'client.id': 'kafka_admin_client'
}

admin_client = AdminClient(admin_config)

def create_new_topic():
    """Checks if the topic office_input exists or not. If not, creates the topic."""
    topic_name = 'email_topic'

    try:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=3)
        admin_client.create_topics([new_topic])
        return f"Topic {topic_name} successfully created"
    except:
        return f"Topic {topic_name} already exists"

if __name__ == "__main__":
    result = create_new_topic()
    print(result)
