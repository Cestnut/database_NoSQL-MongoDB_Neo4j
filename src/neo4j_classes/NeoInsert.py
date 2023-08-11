from neo4j import GraphDatabase

class NeoInsert:
    def __init__(self, database):
        self.database = database

    def init_queries(self):
        insert = """
        LOAD CSV WITH HEADERS FROM 'file:///{}' AS row
        MERGE (e:{} {})
        RETURN count(e)
        """

        insert_people = insert.format("people.csv", "Person", 
                                    "{number:toInteger(row.NUMBER), first_name:row.FIRST_NAME, last_name:row.LAST_NAME}")

        insert_cells = insert.format("cells.csv", "Cell",
                                    "{ID:toInteger(row.ID), city:row.CITY, address:row.ADDRESS}")

        insert_calls = insert.format("calls.csv", "Call",
                                    "{ID:toInteger(row.ID), begin_timestamp:toInteger(row.BEGIN_TIMESTAMP), end_timestamp:toInteger(row.END_TIMESTAMP)}")


        create_caller_called_relationship = """
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        MATCH (c:Call {ID: toInteger(row.ID)})
        MATCH (p1:Person {number:toInteger(row.CALLER)})
        MATCH (p2:Person {number:toInteger(row.CALLED)})
        WHERE toInteger(row.CALLER) = p1.number AND toInteger(row.CALLED) = p2.number
        MERGE (p1)-[:CALLER]->(c)-[:CALLED]->(p2)
        RETURN count(row);"""

        create_cells_relationsip = """
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        MATCH (call:Call {ID: toInteger(row.ID)})
        MATCH (cell:Cell {ID:toInteger(row.CELL_ID)})
        WHERE toInteger(row.CELL_ID) = cell.ID
        MERGE (call)-[:SOURCE]->(cell)
        RETURN count(row);"""

        self.queries = [insert_people, insert_cells, insert_calls, 
                create_caller_called_relationship, create_cells_relationsip]

    def insert_all_data(self, debug=True):
        self.init_queries()
        for query in self.queries:
            result=self.database.execute_query(query)
            if debug:
                print(result.records)