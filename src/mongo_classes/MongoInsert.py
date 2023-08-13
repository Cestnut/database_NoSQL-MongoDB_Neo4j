import pymongo
import csv

class MongoInsert:
    def __init__(self, root_path, database, buffer_size=0):
        
        self.buffer_size = buffer_size
        self.database = database
        self.root_path = root_path

    def insert_many(self, file_name, data_collection, collection_name, debug=True):
        data = list()
        if debug:
            print("Popolando la collezione {}".format(collection_name))
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
                #Permette di svuotare la lista data ogni self.buffer_size elementi caricati, in modo da non appesantire troppo la memoria
                if self.buffer_size != 0:
                    if i%self.buffer_size == 0:
                        data_collection.insert_many(data)
                        data = list()
        #Inserisce alla fine tutti i dati se non era specificata una dimensione del buffer
        if self.buffer_size == 0:
            data_collection.insert_many(data)
            
        if debug:
            print("Popolata la collezione {}\n\n".format(collection_name))
    
    def clear_database(self, debug):
        for collection_name in self.database.list_collection_names():
            result = self.database[collection_name].delete_many({})
            if debug:
                print("Eliminati {} documenti dalla collection {}".format(result.deleted_count, collection_name))

    def insert_all_data(self, debug = True):

        self.clear_database(debug)

        people_collection = self.database["people"]
        cells_collection = self.database["cells"]
        calls_collection = self.database["calls"]

        if debug: print("Creando gli indici")
        people_collection.create_index("NUMBER", unique=True)
        cells_collection.create_index("ID", unique=True)
        calls_collection.create_index("ID", unique=True)

        self.insert_many(self.root_path+"/csv/people.csv", people_collection, "people", debug)
        self.insert_many(self.root_path+"/csv/cells.csv", cells_collection, "cells", debug)
        self.insert_many(self.root_path+"/csv/calls.csv", calls_collection, "calls", debug)