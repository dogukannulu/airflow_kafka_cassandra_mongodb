import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_record_for_cassandra(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task_ids='kafka_consumer_cassandra', key='json_data_cassandra')
    logger.info("XCom Cassandra obtained successfully")

    data_dict = json.loads(json_data)
    
    return data_dict

def get_record_for_mongodb(**kwargs):
    json_data = kwargs['ti'].xcom_pull(task_ids='kafka_consumer_mongodb', key='json_data_mongodb')
    logger.info("XCom MongoDB obtained successfully")

    data_dict = json.loads(json_data)
    
    return data_dict