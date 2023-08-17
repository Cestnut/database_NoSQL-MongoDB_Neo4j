from mongo_classes.MongoHandler import MongoHandler
from neo4j_classes.NeoHandler import NeoHandler
from DataGenerator import DataGenerator

import utils
import subprocess
import os

import ast
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
    if nocache_time:
        no_cache_times.append(["QUERY_1", "QUERY_2", "QUERY_3","QUERY_4","QUERY_5"])
        no_cache_tmp = list()
        i = 1
        for query in database_handler.reader.read_queries:
            database_handler.clear_cache()
            execution_time = database_handler.reader.read_query(query)
            print("Query eseguite (no cache): {}".format(i))
            i+=1
            no_cache_tmp.append(execution_time)
        no_cache_times.append(no_cache_tmp)
        
    #Il seguente serve per popolare la cache delle query
    i = 1
    for query in database_handler.reader.read_queries:
        database_handler.reader.read_query(query)
        print("Query eseguite (popolando la cache): {}".format(i))
        i+=1

    normal_times.append(["QUERY_1", "QUERY_2", "QUERY_3","QUERY_4","QUERY_5"])
    for i in range(iterations):
        normal_iteration = list()
        for query in database_handler.reader.read_queries:
            execution_time = database_handler.reader.read_query(query)
            print("Query eseguite: {}".format(i+1))
            normal_iteration.append(execution_time)
        normal_times.append(normal_iteration)

    return no_cache_times, normal_times



if __name__ == "__main__":

    parser = argparse.ArgumentParser(
                    prog='Test',
                    description='Esegue i test delle query e salva i risultati',
                    )
    
    parser.add_argument('--begin_timestamp', help='Inizio del range di timestamp in cui cercare le chiamate', required=True)
    parser.add_argument('--end_timestamp', help='Fine del range di timestamp in cui cercare le chiamate', required=True)
    parser.add_argument('--city', help='Città delle celle da cui sono partite le chiamate', required=True)
    parser.add_argument('--iterations', help="Numero di iterazioni da eseguire (default: 30)")
    parser.add_argument('--debug', help="Se si vogliono attivare le informazioni di debug (default: True)")
    parser.add_argument('--mongo', help='True/False, se si vogliono eseguire i test per MongoDB (default: True)')
    parser.add_argument('--neo4j', help='True/False, se si vogliono eseguire i test per Neo4j (default: True)')
    parser.add_argument('--mongo_insert_buffer_size', 
                        help="Dimensione del buffer su cui caricare i record prima che vengano inseriti nel database(default: 10000)")
    parser.add_argument('--reset', help="True/False, se resettare i container all'avvio del test (default:False)")
    parser.add_argument('--nocache_time', help="True/False, se fare per la prima volta le query senza usare la cache (default:True)")

    args = parser.parse_args()

    begin_timestamp = int(args.begin_timestamp)
    end_timestamp = int(args.end_timestamp)
    city = args.city
    iterations = int(args.iterations) if args.iterations else 30
    debug = ast.literal_eval(args.debug) if args.debug else True
    mongo = ast.literal_eval(args.mongo) if args.mongo else True
    neo = ast.literal_eval(args.neo4j) if args.neo4j else True
    mongo_insert_buffer_size = int(args.mongo_insert_buffer_size) if args.mongo_insert_buffer_size else 10000
    reset = ast.literal_eval(args.reset) if args.reset else False
    nocache_time = ast.literal_eval(args.nocache_time) if args.nocache_time else True

    data_set_percentage = [25,50,75,100]
    for percentage in data_set_percentage:
        reset_containers()
        utils.copy_dataset(percentage, debug)

        if mongo:
            mongo_handler = MongoHandler(begin_timestamp, end_timestamp, city, utils.root_path, insert_buffer_size=mongo_insert_buffer_size)
            insert_data(mongo_handler, debug=debug)
            result = read(mongo_handler, begin_timestamp, end_timestamp, city, iterations, nocache_time=nocache_time, debug=debug)
            utils.write_results("mongo", percentage, result)
            utils.rearrange_results("mongo")


        if neo:
            neo_handler = NeoHandler(begin_timestamp, end_timestamp, city)
            insert_data(neo_handler, debug=debug)
            result = read(neo_handler, begin_timestamp, end_timestamp, city, iterations, nocache_time=nocache_time, debug=debug)
            utils.write_results("neo4j", percentage, result)
            utils.rearrange_results("neo4j")