from pymongo import MongoClient
import csv

class MongoInsert:
    def __init__(self, root_path="/home/giov/database_NoSQL-MongoDB_Neo4j", 
                 db_name="calls_network", host="localhost", port=27017):
        client = MongoClient("mongodb://{}:{}".format(host,port))
        self.mydb = client[db_name]
        self.root_path = root_path

    def insert_many(self, file_name, collection_name, buffer_size=0, debug=True):
        data_collection = self.mydb[collection_name]
        data = list()
        with open(file_name) as csvfile:
            i = 0
            reader = csv.DictReader(csvfile)
            for row in reader:
                for key in row.keys():
                    #Converte le stringhe contenenti numeri in interi
                    value = row[key]
                    row[key] = int(value) if value.isnumeric() else value
                data.append(row)
                i = i + 1
                #Permette di svuotare la lista data ogni buffer_size elementi caricati, in modo da non appesantire troppo la memoria
                if buffer_size != 0:
                    if i%buffer_size == 0:
                        data_collection.insert_many(data)
                        data = list()
        #Inserisce alla fine tutti i dati se non era specificata una dimensione del buffer
        if buffer_size == 0:
            data_collection.insert_many(data)
            
        if debug:
            print("Popolata la collezione {}".format(collection_name))

    def insert_all_data(self, buffer_size=0, debug = True):
        self.insert_many(self.root_path+"/csv/people.csv", "people", buffer_size, debug)
        self.insert_many(self.root_path+"/csv/cells.csv", "cells", buffer_size, debug)
        self.insert_many(self.root_path+"/csv/calls.csv", "calls", buffer_size, debug)


mongo_inserter = MongoInsert(db_name="calls_network")
mongo_inserter.insert_all_data()
