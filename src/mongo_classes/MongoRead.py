from pymongo import MongoClient
from time import process_time_ns

class MongoRead:
    def __init__(self, begin, end, city,database):
        
        self.calls_collection = database["calls"]
        self.init_queries(begin, end, city)

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