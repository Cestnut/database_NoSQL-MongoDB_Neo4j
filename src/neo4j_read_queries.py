from neo4j import GraphDatabase

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
"""
]

begin = 0
end = 2000000000000000
city = "Siena"
read_queries[0] = read_queries[0].format(begin, end)
read_queries[1] = read_queries[1].format(begin, end)
read_queries[2] = read_queries[2].format(begin, end)
read_queries[3] = read_queries[3].format(begin, end, city)


driver = GraphDatabase.driver("bolt://localhost:7687")
for query in read_queries:
    result = driver.execute_query(query)
    print(result.records)

print(read_queries[3])