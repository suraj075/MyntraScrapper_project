# what Scrapper folder does ?  -->  Helper function that can be used in different parts of projects



from src.cloud_io import MongoIO
from src.constants import MONGO_DATABASE_NAME
from src.exception import CustomException
import os,sys

def fetch_product_name_from_cloud():
    try:
        mongo = MongoIO()
        collection_name = mongo.mongo_ins._mongo_operation_connect_database.list_collection_name()
        return [collection_name.replace('_',' ')
                for collection_name in collection_name]
    
    except Exception as e:
        raise CustomException(e,sys)