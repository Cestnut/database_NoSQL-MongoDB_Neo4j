from faker import Faker
import random
from datetime import datetime, timedelta
import os

class DataGenerator:
    def __init__(self, calls_set_size = 1000, people_set_size = 50, cells_set_size = 50, 
                 calls_min_duration=30, calls_max_duration=3600, data_path="../csv/"):
        
        self.calls_set_size = calls_set_size #Numero di chiamate
        self.people_set_size = people_set_size
        self.cells_set_size = cells_set_size

        self.calls_min_duration = calls_min_duration #Durata minima delle chiamate in secondi
        self.calls_max_duration = calls_max_duration #Durata massima delle chiamate in secondi
        self.call_begin_range = datetime(2023, 1, 1) #Inizio range dell'inizio delle chiamate. Formato: Y/M/S
        self.call_end_range = datetime(2023, 12, 31)

        self.phone_numbers = list() #Lista di tutti i numeri di telefono generati. Servirà a generare le chiamate

        self.data_path = data_path #Path in cui sono scritti i file csv

        Faker.seed(0)
        self.fake = Faker('it_IT')

    def generate_people(self, progress_info=False):
        #NUMERO, NOME, COGNOME
        people = list()
        people.append(["NUMERO", "NOME", "COGNOME"]) #Headers
        for i in range(self.people_set_size):
            person = list()

            #genera un numero di telefono univoco, finché non ne trova uno che non ha il prefisso (per standardizzare il formato del numero)
            while True:
                phone_number = self.fake.unique.phone_number()
                if phone_number[0] == "+":
                    continue
                else:
                    break
            self.phone_numbers.append(phone_number)

            person.append(phone_number)
            person.append(self.fake.first_name())
            person.append(self.fake.last_name())
            #Aggiunge la persona all'insieme di persone
            people.append(person)

            if progress_info:
                if (i+1) % (self.people_set_size//10) == 0:
                    percentage = ((i+1)/self.people_set_size)*100
                    print("Generazione persone: {}%".format(percentage))
        return people

    def generate_cells(self, progress_info=False):
        #ID, CITTA, INDIRIZZO
        cells = list()
        cells.append(["ID", "CITTA", "INDIRIZZO"]) #Headers
        for i in range(1, self.cells_set_size+1):
            cell = list()
            cell.append(i)
            cell.append(self.fake.state())
            cell.append(self.fake.street_name())

            cells.append(cell)

            if progress_info:
                if (i+1) % (self.cells_set_size//10) == 0:
                    percentage = ((i+1)/self.cells_set_size)*100
                    print("Generazione celle: {}%".format(percentage))
        return cells

    def generate_calls(self, progress_info=False):
        #ID, CALLER, CALLED, CELL_ID, BEGIN_TIMESTAMP, END_TIMESTAMP
        calls = list()
        calls.append(["ID", "CALLER", "CALLED", "CELL_ID", "BEGIN_TIMESTAMP", "END_TIMESTAMP"])
        for i in range(1, self.calls_set_size+1):
            call = list()
            call.append(i)
            caller = random.choice(self.phone_numbers)
            while True:
                called = random.choice(self.phone_numbers)
                if caller != called:
                    break
            call.append(caller)
            call.append(called)
            call.append(random.randint(1, self.cells_set_size+1))
            
            #Crea un oggetto datetime con data nel range specificato,
            #e poi ne crea un altro sommando una durata casuale tra il range specificato
            begin_date_time = self.fake.date_time_between_dates(self.call_begin_range, self.call_end_range)
            duration = random.randint(self.calls_min_duration, self.calls_max_duration)
            end_date_time = begin_date_time + timedelta(seconds=duration)
            
            #Converte gli oggetti datetime in interi timestamp
            begin_date_time = int(begin_date_time.timestamp())
            end_date_time = int(end_date_time.timestamp())

            call.append(begin_date_time)
            call.append(end_date_time)

            calls.append(call)

            if progress_info:
                if (i+1) % (self.calls_set_size//10) == 0:
                    percentage = ((i+1)/self.calls_set_size)*100
                    print("Generazione chiamate: {}%".format(percentage))
        return calls


    def write_csv_file(self, filename, list_to_write):
        with open(self.data_path+filename+".csv", "w") as file:
            print("Scrivendo il file {}".format(filename))    
            for record in list_to_write:
                record = [str(value) for value in record]
                newLine = ",".join(record)+"\n"
                file.write(newLine)
            file.close()
            

    def generate(self, people_progress_info=False, cells_progress_info=False, 
                 calls_progress_info=False):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
        
        self.write_csv_file("people", self.generate_people(progress_info=people_progress_info))
        self.write_csv_file("cells", self.generate_cells(progress_info=cells_progress_info))
        self.write_csv_file("calls", self.generate_calls(progress_info=calls_progress_info))

if __name__ == "__main__":
    generator = DataGenerator(calls_set_size=50000, people_set_size=10000, cells_set_size=10000)
    
    generator.generate(people_progress_info=True, 
                       cells_progress_info=True, calls_progress_info=True)