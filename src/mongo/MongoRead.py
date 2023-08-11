from pymongo import MongoClient
from time import process_time_ns

class MongoRead:
    def __init__(self, db_name="calls_network", host="localhost", port=27017, calls_collection_name="calls"):
        client = MongoClient("mongodb://{}:{}".format(host,port))
        mydb = client[db_name]
        self.calls_collection = mydb[calls_collection_name]

    def init_queries(self, begin, end, city):
        self.read_queries = [
            [
                {
                    "$match":{
                        "BEGIN_TIMESTAMP":{"$gte": begin, "$lte": end}
                    }
                }
            ],
            [
                {
                    "$lookup":{
                        "from":"people",
                        "localField": "CALLER",
                        "foreignField": "NUMBER",
                        "as":"caller"
                        }
                },
                {
                    "$match":{
                        "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
                    }
                }
            ],
            [
            {
                    "$lookup":{
                        "from":"people",
                        "localField": "CALLER",
                        "foreignField": "NUMBER",
                        "as":"caller"
                        }
                },{
                    "$match":{
                        "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
                    }
                },{
                    "$lookup":{
                        "from":"cells",
                        "localField": "CELL_ID",
                        "foreignField": "ID",
                        "as":"cell"
                        }
                }
            ],[
            {
                    "$lookup":{
                        "from":"people",
                        "localField": "CALLER",
                        "foreignField": "NUMBER",
                        "as":"caller"
                        }
                },{
                    "$match":{
                        "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
                    }
                },{
                    "$lookup":{
                        "from":"cells",
                        "localField": "CELL_ID",
                        "foreignField": "ID",
                        "as":"cell",
                        "pipeline":[
                            {
                                "$match":{"CITY":{"$eq":city}}
                            }
                        ]
                        }
                },{
                    "$match":{"cell":{"$ne":[]}}
                }
            ],[
            {
                    "$lookup":{
                        "from":"people",
                        "localField": "CALLER",
                        "foreignField": "NUMBER",
                        "as":"caller"
                        }
                },{
                    "$match":{
                        "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
                    }
                },{
                    "$lookup":{
                        "from":"cells",
                        "localField": "CELL_ID",
                        "foreignField": "ID",
                        "as":"cell",
                        "pipeline":[
                            {
                                "$match":{"CITY":{"$eq":city}}
                            }
                        ]
                        }
                },{
                    "$match":{"cell":{"$ne":[]}}
                },{
                    "$lookup":{
                        "from":"people",
                        "localField": "CALLED",
                        "foreignField": "NUMBER",
                        "as":"called"
                        }
                }
            ]
        ]

    #Ritorna il tempo di esecuzione della query in millisecondi
    def read_query(self, query):
        begin_time = process_time_ns()
        result = self.calls_collection.aggregate(query)
        end_time = process_time_ns()
        time_elapsed = (end_time-begin_time)//(10**6)
        return time_elapsed

    #Ritorna una lista col tempo di esecuzione di tutte le query
    def all_read_queries(self, begin, end, city):
        self.init_queries(begin, end, city)
        times_list = list()
        for query in self.read_queries:
            times_list.append(self.read_query(query))
        return times_list

begin = 0
end = 2000000000000000
city = "Imperia"

mongo_read = MongoRead(db_name="calls_network", calls_collection_name="calls")
times = mongo_read.all_read_queries(begin, end, city)
print(times)
