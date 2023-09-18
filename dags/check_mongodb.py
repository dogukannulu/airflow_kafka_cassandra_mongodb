from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB configuration
mongodb_uri = 'mongodb://root:root@mongo:27017/'
database_name = 'email_namespace'
collection_name = 'email_table'

# Connect to MongoDB and the specified collection
client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

# Define a sample email to search for
sample_email = 'dogu@example.com'

# Query the collection for the sample email
result = collection.find_one({'email': sample_email})

# Check if the result is not None (i.e., data was found)
if result:
    logger.info(f"Data found for email: {result['email']}")
    logger.info(f"OTP: {result['otp']}")
else:
    logger.error(f"No data found for email: {sample_email}")

# Close the MongoDB connection
client.close()
