from mongo_classes.MongoHandler import MongoHandler
from neo4j_classes.NeoHandler import NeoHandler
from DataGenerator import DataGenerator

import utils
import subprocess
import os

#Distrugge i container e li ricrea, difatti cancellando il contenuto dei database e la cache
def reset():
    p = subprocess.Popen([utils.root_path+"/start.sh"], shell=True)
    p.wait()

def insert_data(database_handler, buffer_size=0, debug=True):
    database_handler.inserter.insert_all_data(debug=debug)

def read(database_handler, begin, end, city, iterations, nocache_time=False, debug=True):
    no_cache_times = list()
    normal_times = list()
    j = 0
    if nocache_time:
        no_cache_times.append(["QUERY 1", "QUERY 2", "QUERY 3","QUERY 4","QUERY 5"])
        no_cache_tmp = list()
        for query in database_handler.reader.read_queries:
            database_handler.clear_cache()
            execution_time = database_handler.reader.read_query(query)
            j+=1
            print("Queried {} times".format(j))
            no_cache_tmp.append(execution_time)
        no_cache_times.append(no_cache_tmp)

    normal_times.append(["QUERY 1", "QUERY 2", "QUERY 3","QUERY 4","QUERY 5"])
    for i in range(iterations):
        normal_iteration = list()
        for query in database_handler.reader.read_queries:
            j+=1
            print("Queried {} times".format(j))
            execution_time = database_handler.reader.read_query(query)
            normal_iteration.append(execution_time)
        normal_times.append(normal_iteration)

    return no_cache_times, normal_times

def write_results(dbms, times):
    no_cache_times = times[0]
    normal_times = times[1]
    results_path = utils.root_path + "/results/" + dbms

    if not os.path.exists(results_path):
            os.makedirs(results_path)

    #Controlla se ci siano altri campi oltre agli header
    if len(no_cache_times) > 1:
        print(no_cache_times)
        utils.write_csv(results_path+"/nocache", no_cache_times)
    
    #Controlla se ci siano altri campi oltre agli header
    if len(normal_times) > 1:
        utils.write_csv(results_path+"/cache", normal_times)

mongo_insert_buffer_size = 0

begin = 0
end = 2000000000000000
city = "Imperia"
debug = True
iterations = 30
nocache_time = True
mongo=True
neo=True

reset()

if mongo:
    mongo_handler = MongoHandler(begin, end, city, utils.root_path, insert_buffer_size=mongo_insert_buffer_size)
    insert_data(mongo_handler, debug=debug)
    result = read(mongo_handler, begin, end, city, iterations, nocache_time=nocache_time, debug=debug)
    write_results("mongo", result)

if neo:
    neo_handler = NeoHandler(begin, end, city)
    insert_data(neo_handler, debug=debug)
    result = read(neo_handler, begin, end, city, iterations, nocache_time=nocache_time, debug=debug)
    write_results("neo4j", result)