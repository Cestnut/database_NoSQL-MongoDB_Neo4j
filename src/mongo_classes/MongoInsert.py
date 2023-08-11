from pymongo import MongoClient
import csv

class MongoInsert:
    def __init__(self, root_path, database, buffer_size=0):
        
        self.buffer_size = buffer_size
        self.database = database
        self.root_path = root_path

    def insert_many(self, file_name, collection_name, debug=True):
        data_collection = self.database[collection_name]
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
                #Permette di svuotare la lista data ogni self.buffer_size elementi caricati, in modo da non appesantire troppo la memoria
                if self.buffer_size != 0:
                    if i%self.buffer_size == 0:
                        data_collection.insert_many(data)
                        data = list()
        #Inserisce alla fine tutti i dati se non era specificata una dimensione del buffer
        if self.buffer_size == 0:
            data_collection.insert_many(data)
            
        if debug:
            print("Popolata la collezione {}".format(collection_name))

    def insert_all_data(self, debug = True):
        self.insert_many(self.root_path+"/csv/people.csv", "people", debug)
        self.insert_many(self.root_path+"/csv/cells.csv", "cells", debug)
        self.insert_many(self.root_path+"/csv/calls.csv", "calls", debug)