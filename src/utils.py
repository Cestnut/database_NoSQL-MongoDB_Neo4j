from datetime import datetime
import subprocess
import csv
import os
root_path = "/home/giov/database_NoSQL-MongoDB_Neo4j"
original_csv = root_path + "/csv/csv_original/"
input_csv = root_path + "/csv/input_csv/"
results_csv = root_path + "/results"

def write_csv(file_path, csv_list):
    with open(file_path+".csv", "w") as file:
            print("Scrivendo il file {}".format(file_path))    
            for record in csv_list:
                record = [str(value) for value in record]
                newLine = ",".join(record)+"\n"
                file.write(newLine)
            print("Scritto il file {}\n\n".format(file_path))    
            file.close()

#Ritorna il numero di record nel file csv (escludendo la prima linea con gli head)
def csv_len(file_path):
    p = subprocess.run(["wc", "-l", file_path], stdout=subprocess.PIPE, text=True)
    output = int(p.stdout.strip().split()[0]) - 1
    return output

def copy_dataset(percentage, debug):

    if not os.path.exists(input_csv):
            os.makedirs(input_csv)
    
    if debug: print("Copiando il dataset")
    for file_name in os.listdir(original_csv):
        with open(original_csv+file_name, 'r') as input_file,  open(input_csv+file_name, 'w') as output_file:

            reader = csv.reader(input_file)
            writer = csv.writer(output_file)

            input_len = csv_len(original_csv+file_name)
            output_len = int((input_len*(percentage/100)) + 1)

            for _ in range(output_len):
                row = next(reader)
                writer.writerow(row)
    if debug: print("Finito di copiare il dataset")

def write_results(dbms, size_percentage, times):
    no_cache_times = times[0]
    normal_times = times[1]
    results_path = results_csv + "/original/" + dbms
    results_path_nocache = results_path + "/nocache/"
    results_path_cache = results_path + "/cache/"
    
    if not os.path.exists(results_path_nocache):
            os.makedirs(results_path_nocache)

    if not os.path.exists(results_path_cache):
            os.makedirs(results_path_cache)    

    #Controlla se ci siano altri campi oltre agli header
    if len(no_cache_times) > 1:
        write_csv(results_path_nocache+dbms+str(size_percentage), no_cache_times)
    
    #Controlla se ci siano altri campi oltre agli header
    if len(normal_times) > 1:
        write_csv(results_path_cache+dbms+str(size_percentage), normal_times)

#In modo da rendere più facile l'importazione e la manipolazione dei risultati su calc
def rearrange_results(dbms):

    source_path = results_csv + "/original/"+dbms
    destination_path = results_csv + "/rearranged/"+dbms
    
    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    for destination in ["QUERY_1", "QUERY_2", "QUERY_3", "QUERY_4", "QUERY_5"]:
        destination_file = open(destination_path+"/"+destination+".csv", "w")
        cols = list()
        for source in ["25.csv", "50.csv", "75.csv", "100.csv"]:
            col = list()
            for test_type in ["/nocache/", "/cache/"]:
                source_filename = source_path+test_type+dbms+source
                source_file = open(source_filename)
                #print("\n\nSource: {}".format(source_filename))
                for row in csv.DictReader(source_file):
                    col.append(row[destination])
            cols.append(col)

        destination_file.write("25%,50%,75%,100%\n")
        for i in range(len(cols[0])):
            newline = list()
            for col in cols:
                newline.append(col[i])
            newline = [str(val) for val in newline]
            newline = ",".join(newline) + "\n"
            destination_file.write(newline)
        destination_file.close()

#Ritorna oggetto datetime dalla string (formato YYYY/MM/DD)
def parse_date(date_string):
    date_string = [int(val) for val in date_string.split("/")]
    return datetime(date_string[0], date_string[1], date_string[2])

