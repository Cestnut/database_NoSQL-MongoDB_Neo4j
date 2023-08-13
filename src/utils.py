from datetime import datetime
root_path = "/home/giov/database_NoSQL-MongoDB_Neo4j"

def write_csv(file_path, csv_list):
    with open(file_path+".csv", "w") as file:
            print("Scrivendo il file {}".format(file_path))    
            for record in csv_list:
                record = [str(value) for value in record]
                newLine = ",".join(record)+"\n"
                file.write(newLine)
            print("Scritto il file {}\n\n".format(file_path))    
            file.close()

#Ritorna oggetto datetime dalla string (formato YYYY/MM/DD)
def parse_date(date_string):
    date_string = [int(val) for val in date_string.split("/")]
    return datetime(date_string[0], date_string[1], date_string[2])