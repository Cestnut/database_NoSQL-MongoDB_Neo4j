from neo4j import GraphDatabase
from time import process_time_ns

class NeoRead:
    def __init__(self, host="localhost", port="7687"):
        self.driver = GraphDatabase.driver("bolt://{}:{}".format(host,port))
    
    def init_queries(self, begin, end, city):
        read_queries = [
        """MATCH (call:Call) 
        WHERE call.begin_timestamp > {} AND call.begin_timestamp < {} 
        RETURN call
        ""","""
        MATCH (person:Person)-[:CALLER]->(call:Call) 
        WHERE call.begin_timestamp > {} AND call.begin_timestamp < {} 
        RETURN person,call
        ""","""
        MATCH (person:Person)-[:CALLER]->(call:Call)-[:SOURCE]->(cell:Cell)
        WHERE call.begin_timestamp > {} AND call.begin_timestamp < {} 
        RETURN person,call,cell
        ""","""
        MATCH (person:Person)-[:CALLER]->(call:Call)-[:SOURCE]->(cell:Cell)
        WHERE call.begin_timestamp > {} AND call.begin_timestamp < {} AND cell.city = '{}'
        RETURN person,call,cell
        ""","""
        MATCH (person1:Person)-[:CALLER]->(call:Call)-[:SOURCE]->(cell:Cell)
        MATCH (call:Call)-[:CALLED]->(person2:Person)
        WHERE call.begin_timestamp > {} AND call.begin_timestamp < {} AND cell.city = '{}'
        RETURN person1,person2,call,cell
        """
        ]
        read_queries[0] = read_queries[0].format(begin, end)
        read_queries[1] = read_queries[1].format(begin, end)
        read_queries[2] = read_queries[2].format(begin, end)
        read_queries[3] = read_queries[3].format(begin, end, city)
        read_queries[4] = read_queries[4].format(begin, end, city)
        self.read_queries = read_queries

    #Ritorna il tempo di esecuzione della query in millisecondi
    def read_query(self, query):
        begin_time = process_time_ns()
        result = self.driver.execute_query(query)
        end_time = process_time_ns()
        time_elapsed = (end_time-begin_time)//(10**6)
        return time_elapsed
    
    def all_read_queries(self, begin, end, city):
        self.init_queries(begin, end, city)
        times_list = list()
        for query in self.read_queries:
            times_list.append(self.read_query(query))
        return times_list    

begin = 0
end = 2000000000000000
city = "Imperia"

neo_reader = NeoRead()
times = neo_reader.all_read_queries(begin,end,city)
print(times)