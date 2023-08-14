from datetime import datetime
import subprocess
import csv
import os
root_path = "/home/giov/database_NoSQL-MongoDB_Neo4j"
original_csv = root_path + "/csv/csv_original/"
input_csv = root_path + "/csv/input_csv/"

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
    
#Ritorna oggetto datetime dalla string (formato YYYY/MM/DD)
def parse_date(date_string):
    date_string = [int(val) for val in date_string.split("/")]
    return datetime(date_string[0], date_string[1], date_string[2])

