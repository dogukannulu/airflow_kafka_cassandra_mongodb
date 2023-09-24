import pymongo
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_last_email_and_otp_mongodb():
    mongodb_uri = 'mongodb://root:root@mongo:27017/'
    database_name = 'email_database'
    collection_name = 'email_collection'
    
    client = pymongo.MongoClient(mongodb_uri)
    database = client[database_name]
    
    try:
        collection = database[collection_name]
        
        last_document = collection.find_one({}, sort=[('_id', pymongo.DESCENDING)])
        
        if last_document:
            email = last_document.get("email")
            top = last_document.get("top")
            logger.info("Records obtained successfully")
            return email, top
        else:
            return None, None
    except Exception as e:
        logger.warning(f"An error occurred: {e}")
    finally:
        client.close()