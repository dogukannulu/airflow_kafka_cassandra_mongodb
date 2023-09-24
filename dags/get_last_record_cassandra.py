import logging
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def get_last_email_and_otp_cassandra():
    cassandra_hosts = ['cassandra']
    
    cluster = Cluster(contact_points=cassandra_hosts)
    session = cluster.connect()
    
    try:
        cql_query = "SELECT * FROM email_namespace.email_table LIMIT 1 ALLOW FILTERING"
        
        result = session.execute(cql_query)
        
        for row in result:
            email = row.email
            otp = row.otp
            logger.info("Records obtained successfully")
            return email, otp
        
        return None, None
    except Exception as e:
        logger.warning(f"An error occurred: {e}")
    finally:
        cluster.shutdown()