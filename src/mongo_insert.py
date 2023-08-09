from pymongo import MongoClient
import csv


def insert_many(mydb, file_name, collection_name, chunk_size=0):
    data = list()
    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)

    data_collection = mydb[collection_name]
    data_collection.insert_many(data)
    print("Popolata la collezione {}".format(collection_name))



client = MongoClient("mongodb://localhost:27017")
client.drop_database("calls_network")
mydb = client["calls_network"]
insert_many(mydb, "../csv/people.csv", "people")
insert_many(mydb, "../csv/cells.csv", "cells")
insert_many(mydb, "../csv/calls.csv", "calls")
