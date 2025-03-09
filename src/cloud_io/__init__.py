# # what Scrapper folder does ?  --> Handles the cloud related input/output
# import pandas as pd
# # from database_connect import mongo_operation as mongo 
# from database_connect import mongo_operation as mongo
# import os, sys
# from src.constants import *
# from src.exception import CustomException


# # class MongoIO - Interact with mongoDB database for storing and retriving product reviews.
# # mongo_ins - class level variable, establish only once during life time of the application
# class MongoIO:
#     mongo_ins = None

#     def __init__(self):
#         if MongoIO.mongo_ins is None:
#             mongo_db_url = "mongodb+srv://bihtadanapur6:Aq7SfWTvjKLkKHyU@cluster0.8ckbf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#             if mongo_db_url is None:
#                 raise Exception(f"Environment key: {MONGODB_URL_KEY} is not set.")
#             MongoIO.mongo_ins = mongo(client_url=mongo_db_url,
#                                       database_name=MONGO_DATABASE_NAME)
#         self.mongo_ins = MongoIO.mongo_ins

#     def store_reviews(self,
#                       product_name: str, reviews: pd.DataFrame):
#         try:
#             collection_name = product_name.replace(" ", "_")
#             self.mongo_ins.bulk_insert(reviews,
#                                        collection_name)

#         except Exception as e:
#             raise CustomException(e, sys)

#     def get_reviews(self,
#                     product_name: str):
#         try:
#             data = self.mongo_ins.find(
#                 collection_name=product_name.replace(" ", "_")
#             )

#             return data

#         except Exception as e:
#             raise CustomException(e, sys)





import pandas as pd
import os, sys
import pymongo
from src.constants import *
from src.exception import CustomException
from pymongo import MongoClient

class MongoIO:
    mongo_ins = None

    def __init__(self):
        if MongoIO.mongo_ins is None:
            mongo_db_url = "mongodb+srv://bihtadanapur6:Aq7SfWTvjKLkKHyU@cluster0.8ckbf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
            if mongo_db_url is None:
                raise Exception(f"Environment key: {MONGODB_URL_KEY} is not set.")
            MongoIO.mongo_ins = MongoClient(mongo_db_url)
        self.mongo_ins = MongoIO.mongo_ins
        self.db = self.mongo_ins[MONGO_DATABASE_NAME]

    def store_reviews(self, product_name: str, reviews: pd.DataFrame):
        try:
            collection_name = product_name.replace(" ", "_")
            collection = self.db[collection_name]
            
            # Convert DataFrame to list of dictionaries for MongoDB insertion
            records = reviews.to_dict('records')
            if records:
                collection.insert_many(records)
            
        except Exception as e:
            raise CustomException(e, sys)

    def get_reviews(self, product_name: str):
        try:
            collection_name = product_name.replace(" ", "_")
            collection = self.db[collection_name]
            
            # Fetch all documents from the collection
            cursor = collection.find({})
            
            # Convert to DataFrame
            data = pd.DataFrame(list(cursor))
            
            # Drop MongoDB's _id column if it exists
            if '_id' in data.columns:
                data = data.drop('_id', axis=1)
                
            return data
            
        except Exception as e:
            raise CustomException(e, sys)
    
    def get_product_names(self):
        """
        Get list of all product names (collection names) from MongoDB
        
        Returns:
            list: List of product names
        """
        try:
            # Get all collection names from the database
            collection_names = self.db.list_collection_names()
            
            # Convert collection names back to product names (replace underscores with spaces)
            product_names = [name.replace("_", " ") for name in collection_names]
            
            return product_names
            
        except Exception as e:
            raise CustomException(e, sys)