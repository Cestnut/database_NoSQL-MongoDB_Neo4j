from neo4j import GraphDatabase
"""
Neo4J Insert queries

LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row
MERGE (e:Person {number:row.NUMBER, first_name:row.FIRST_NAME, last_name:row.LAST_NAME})
RETURN count(e)

LOAD CSV WITH HEADERS FROM 'file:///cells.csv' AS row
MERGE (e:Cell {ID:row.ID, city:row.CITY, address:row.ADDRESS})
RETURN count(e)

LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MERGE (e:Call {ID:row.ID, begin_timestamp:row.BEGIN_TIMESTAMP, end_timestamp:row.END_TIMESTAMP})
RETURN count(e)

LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MATCH (c:Call {ID: row.ID})
MATCH (p1:Person {number:row.CALLER})
MATCH (p2:Person {number:row.CALLED})
WHERE row.CALLER = p1.number AND row.CALLED = p2.number
MERGE (p1)-[:CALLER]->(c)-[:CALLED]->(p2)
RETURN count(row);

LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MATCH (call:Call {ID: row.ID})
MATCH (cell:Cell {ID:row.CELL_ID})
WHERE row.CELL_ID = cell.ID
MERGE (call)-[:SOURCE]->(cell)
RETURN count(row);
"""

insert_people = """LOAD CSV WITH HEADERS FROM 'file:///people.csv' AS row MERGE 
                (e:Person {number:row.NUMBER, first_name:row.FIRST_NAME, last_name:row.LAST_NAME}) 
                RETURN count(e)"""

insert_cells = """
LOAD CSV WITH HEADERS FROM 'file:///cells.csv' AS row
MERGE (e:Cell {ID:row.ID, city:row.CITY, address:row.ADDRESS})
RETURN count(e)"""

insert_calls = """LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MERGE (e:Call {ID:row.ID, begin_timestamp:row.BEGIN_TIMESTAMP, end_timestamp:row.END_TIMESTAMP})
RETURN count(e)"""

insert_caller_called_relationship = """LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MATCH (c:Call {ID: row.ID})
MATCH (p1:Person {number:row.CALLER})
MATCH (p2:Person {number:row.CALLED})
WHERE row.CALLER = p1.number AND row.CALLED = p2.number
MERGE (p1)-[:CALLER]->(c)-[:CALLED]->(p2)
RETURN count(row);"""

insert_cells_relationsip = """LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
MATCH (call:Call {ID: row.ID})
MATCH (cell:Cell {ID:row.CELL_ID})
WHERE row.CELL_ID = cell.ID
MERGE (call)-[:SOURCE]->(cell)
RETURN count(row);"""

queries = [insert_people, insert_cells, insert_calls, 
           insert_caller_called_relationship, insert_cells_relationsip]

driver = GraphDatabase.driver("bolt://localhost:7687")
for query in queries:
    driver.execute_query(query)