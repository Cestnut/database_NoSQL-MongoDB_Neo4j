from mongo_classes.MongoHandler import MongoHandler
from neo4j_classes.NeoHandler import NeoHandler
from DataGenerator import DataGenerator

import utils
import subprocess
import os

import argparse

#Distrugge i container e li ricrea, difatti cancellando il contenuto dei database e la cache
def reset_containers():
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                    prog='Test',
                    description='Esegue i test delle query e salva i risultati',
                    )
    
    parser.add_argument('--begin_timestamp', help='Inizio del range di timestamp in cui cercare le chiamate', required=True)
    parser.add_argument('--end_timestamp', help='Fine del range di timestamp in cui cercare le chiamate', required=True)
    parser.add_argument('--city', help='Città delle celle da cui sono partite le chiamate', required=True)
    parser.add_argument('--iterations', help="Numero di iterazioni da eseguire", required=True)
    parser.add_argument('--debug', help="Se si vogliono attivare le informazioni di debug (default: True)")
    parser.add_argument('--mongo', help='True/False, se si vogliono eseguire i test per MongoDB (default: True)')
    parser.add_argument('--neo4j', help='True/False, se si vogliono eseguire i test per Neo4j (default: True)')
    parser.add_argument('--mongo_insert_buffer_size', 
                        help="Dimensione del buffer su cui caricare i record prima che vengano inseriti nel database(default: 10000)")
    parser.add_argument('--reset', help="True/False, se resettare i container all'avvio del test (default:True)")
    parser.add_argument('--nocache_time', help="True/False, se fare per la prima volta le query senza usare la cache (default:True)")

    args = parser.parse_args()

    begin_timestamp = int(args.begin_timestamp)
    end_timestamp = int(args.end_timestamp)
    city = args.city
    iterations = int(args.iterations)
    debug = bool(args.debug) if args.debug else True
    mongo = bool(args.mongo) if args.mongo else True
    neo = bool(args.neo4j) if args.neo4j else True
    mongo_insert_buffer_size = int(args.mongo_insert_buffer_size) if args.mongo_insert_buffer_size else 10000
    reset = bool(args.reset) if args.reset else True
    nocache_time = bool(args.nocache_time) if args.nocache_time else True

    if reset:
        reset_containers()

    if mongo:
        mongo_handler = MongoHandler(begin_timestamp, end_timestamp, city, utils.root_path, insert_buffer_size=mongo_insert_buffer_size)
        insert_data(mongo_handler, debug=debug)
        result = read(mongo_handler, begin_timestamp, end_timestamp, city, iterations, nocache_time=nocache_time, debug=debug)
        write_results("mongo", result)

    if neo:
        neo_handler = NeoHandler(begin_timestamp, end_timestamp, city)
        insert_data(neo_handler, debug=debug)
        result = read(neo_handler, begin_timestamp, end_timestamp, city, iterations, nocache_time=nocache_time, debug=debug)
        write_results("neo4j", result)