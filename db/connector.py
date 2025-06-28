import pymongo

class mongo_connector:

        def __init__(self, ip, puerto, user, password):
                self.client = pymongo.MongoClient(f"mongodb://{user}:{password}@{ip}:{puerto}/")
        
        def get_db(self, db_name):
                return self.client[db_name]

        def get_collection(self, db, coll_name):
                return db[coll_name]

        def insertOne_into_collection(self, collection, data_insert):
                insert = collection.insert_one(data_insert)

        def insertMany_into_collection(self, collection, data_insert):
                insert = collection.insert_many(data_insert)


