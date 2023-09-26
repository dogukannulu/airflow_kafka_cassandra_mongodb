from cassandra.cluster import Cluster
import logging
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect()

    def select_data(self, email):
        query = "SELECT * FROM email_namespace.email_table WHERE email = %s"
        result = self.session.execute(query, (email,))
        
        data_dict = {}
        
        for row in result:
            data_dict['email'] = row.email
            data_dict['otp'] = row.otp
            logger.info(f"Email: {row.email}, OTP: {row.otp}")
        
        if len(data_dict) == 0:
            data_dict['email'] = ''
            data_dict['otp'] = ''
        
        return data_dict

    def close(self):
        self.cluster.shutdown()


def check_cassandra_main():
    cassandra_connector = CassandraConnector(['cassandra'])

    sample_email = 'sample_email@my_email.com'

    data_dict = cassandra_connector.select_data(sample_email)

    cassandra_connector.close()

    logger.info(f"Data found for email: {data_dict['email']}")
    logger.info(f"OTP: {data_dict['otp']}")
    
    return data_dict
