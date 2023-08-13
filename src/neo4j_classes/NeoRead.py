from neo4j import GraphDatabase
from time import process_time_ns

class NeoRead:
    def __init__(self, begin, end, city, driver):
        self.driver = driver
        self.init_queries(begin, end, city)

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
        with self.driver.session() as session:
            begin_time = process_time_ns()
            session.run(query)
            end_time = process_time_ns()
            time_elapsed = (end_time-begin_time)//(10**6)
            return time_elapsed
        