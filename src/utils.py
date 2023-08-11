root_path = "/home/giov/database_NoSQL-MongoDB_Neo4j"

def write_csv(file_path, csv_list):
    with open(file_path+".csv", "w") as file:
            print("Scrivendo il file {}\n\n".format(file_path))    
            for record in csv_list:
                record = [str(value) for value in record]
                newLine = ",".join(record)+"\n"
                file.write(newLine)
            file.close()