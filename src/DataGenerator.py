from faker import Faker
import random
from datetime import datetime, timedelta
import os
import argparse
import utils

class DataGenerator:
    def __init__(self, root_path, people_set_size, cells_set_size, calls_set_size, 
                 call_begin_range, call_end_range,  
                 calls_min_duration, calls_max_duration):
        
        self.calls_set_size = calls_set_size #Numero di chiamate
        self.people_set_size = people_set_size
        self.cells_set_size = cells_set_size

        self.calls_min_duration = calls_min_duration #Durata minima delle chiamate in secondi
        self.calls_max_duration = calls_max_duration #Durata massima delle chiamate in secondi
        self.call_begin_range = call_begin_range
        self.call_end_range = call_end_range

        self.phone_numbers = list() #Lista di tutti i numeri di telefono generati. Servirà a generare le chiamate

        self.data_path = root_path+"/csv_original/" #Path in cui sono scritti i file csv

        Faker.seed(0)
        self.fake = Faker('it_IT')

    def generate_people(self, progress_info=False):
        #NUMERO, NOME, COGNOME
        people = list()
        people.append(["NUMBER", "FIRST_NAME", "LAST_NAME"]) #Headers
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
        cells.append(["ID", "CITY", "ADDRESS"]) #Headers
        for i in range(1, self.cells_set_size+1):
            cell = list()
            cell.append(i)
            cell.append(self.fake.state())
            cell.append(self.fake.street_name())

            cells.append(cell)

            if progress_info:
                if i % (self.cells_set_size//10) == 0:
                    percentage = (i/self.cells_set_size)*100
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
            call.append(random.randint(1, self.cells_set_size))
            
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
                if i % (self.calls_set_size//10) == 0:
                    percentage = (i/self.calls_set_size)*100
                    print("Generazione chiamate: {}%".format(percentage))
        return calls

    def generate(self, people_progress_info=False, cells_progress_info=False, 
                 calls_progress_info=False):
        if not os.path.exists(utils.original_csv):
            os.makedirs(utils.original_csv)
        
        utils.write_csv(utils.original_csv+"/people", self.generate_people(progress_info=people_progress_info))
        utils.write_csv(utils.original_csv+"/cells", self.generate_cells(progress_info=cells_progress_info))
        utils.write_csv(utils.original_csv+"/calls", self.generate_calls(progress_info=calls_progress_info))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='DataGenerator',
                    description='Genera il dataset',
                    )
    parser.add_argument('--people', help='Dimensione del dataset people', required=True)
    parser.add_argument('--cells', help='Dimensione del dataset cells', required=True)
    parser.add_argument('--calls', help='Dimensione del dataset calls', required=True)
    parser.add_argument('--begin_date', help="Data dopo la quale sono avvenute tutte le telefonate (formato YYYY/MM/DD)", required=True)
    parser.add_argument('--end_date', help="Data prima delle quale sono partite tutte le chiamate (formato YYYY/MM/DD)", required=True)
    parser.add_argument('--calls_min_duration', help='Durata minima delle chiamate in secondi (default: 30)')
    parser.add_argument('--calls_max_duration', help='Durata massima delle chiamate in secondi (default: 3600)')
    
    args = parser.parse_args()
    people_set_size = int(args.people)
    cells_set_size = int(args.cells)
    calls_set_size = int(args.calls)

    begin_date = utils.parse_date(args.begin_date)
    end_date = utils.parse_date(args.end_date)

    calls_min_duration = args.calls_min_duration if args.calls_min_duration else 30
    calls_max_duration = args.calls_max_duration if args.calls_max_duration else 3600

    data_generator = DataGenerator(utils.root_path, people_set_size, cells_set_size, calls_set_size,
                                   begin_date, end_date,
                                   calls_min_duration, calls_max_duration)
    
    data_generator.generate(people_progress_info=True, cells_progress_info=True, calls_progress_info=True)