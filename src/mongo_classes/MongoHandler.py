from mongo_classes.MongoInsert import MongoInsert
from mongo_classes.MongoRead import MongoRead
from pymongo import MongoClient

class MongoHandler:
    def __init__(self, begin, end, city, root_path, 
                 db_name="calls_network", host="localhost", port=27017, insert_buffer_size=0):
        
        client = MongoClient("mongodb://{}:{}".format(host,port))
        self.database = client[db_name]

        self.inserter = MongoInsert(root_path, self.database, buffer_size=insert_buffer_size)
        self.reader = MongoRead(begin, end, city, self.database)

    def clear_cache(self):
        self.database.command("planCacheClear", "people")
        self.database.command("planCacheClear", "cells")
        self.database.command("planCacheClear", "calls")

        
