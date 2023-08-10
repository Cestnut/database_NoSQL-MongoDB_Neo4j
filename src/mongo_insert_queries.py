from pymongo import MongoClient
import csv


def insert_many(mydb, file_name, collection_name, buffer_size=0):
    data_collection = mydb[collection_name]
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
        
    print("Popolata la collezione {}".format(collection_name))


client = MongoClient("mongodb://localhost:27017")
client.drop_database("calls_network")
mydb = client["calls_network"]
insert_many(mydb, "../csv/people.csv", "people")
insert_many(mydb, "../csv/cells.csv", "cells")
insert_many(mydb, "../csv/calls.csv", "calls")
