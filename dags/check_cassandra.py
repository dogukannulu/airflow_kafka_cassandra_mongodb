from cassandra.cluster import Cluster
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CassandraConnector:
    def __init__(self, contact_points, keyspace):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)

    def select_data(self, email):
        query = "SELECT * FROM email_namespace.email_table WHERE email = %s"
        result = self.session.execute(query, (email,))
        
        for row in result:
            logger.info(f"Email: {row.email}, OTP: {row.otp}")
        
        # Calculate the length of the SELECT statement
        query_length = len(query)
        logger.info(f"Length of the SELECT statement: {query_length} characters")
        
        return query_length

    def close(self):
        self.cluster.shutdown()

# Cassandra configuration
contact_points = ['cassandra']  # Replace with your Cassandra node(s)
keyspace = 'email_namespace'

# Create a CassandraConnector instance
cassandra_connector = CassandraConnector(contact_points, keyspace)

# Define a sample email to search for
sample_email = 'sample_email@my_email.com'

# Query the Cassandra table for the sample email
cassandra_connector.select_data(sample_email)

query_length = cassandra_connector.select_data(sample_email)

if query_length > 0:
    logger.info("Record successfully inserted into the table")
else:
    logger.error("No records found")

# Close the Cassandra connection
cassandra_connector.close()
