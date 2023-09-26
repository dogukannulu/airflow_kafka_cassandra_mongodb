from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_mongodb_main():
    mongodb_uri = 'mongodb://root:root@mongo:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'

    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    sample_email = 'sample_email@my_email.com'

    result = collection.find_one({'email': sample_email})
    data_dict = {}

    if result:
        logger.info(f"Data found for email: {result['email']}")
        logger.info(f"OTP: {result['otp']}")
        
        data_dict['email'] = result.get('email')
        data_dict['otp'] = result.get('otp')
        
        client.close()
    else:
        data_dict['email'] = ''
        data_dict['otp'] = ''

        client.close()
    return data_dict
