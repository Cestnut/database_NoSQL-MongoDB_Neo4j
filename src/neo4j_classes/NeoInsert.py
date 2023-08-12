from neo4j import GraphDatabase

class NeoInsert:
    def __init__(self, driver):
        self.driver = driver

    def init_queries(self):
        insert = """
        USING PERIODIC COMMIT 1000
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
        USING PERIODIC COMMIT 1000
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        MATCH (c:Call {ID: toInteger(row.ID)})
        MATCH (p1:Person {number:toInteger(row.CALLER)})
        MATCH (p2:Person {number:toInteger(row.CALLED)})
        WHERE toInteger(row.CALLER) = p1.number AND toInteger(row.CALLED) = p2.number
        MERGE (p1)-[:CALLER]->(c)-[:CALLED]->(p2)
        RETURN count(row);"""

        create_cells_relationsip = """
        USING PERIODIC COMMIT 1000
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        MATCH (call:Call {ID: toInteger(row.ID)})
        MATCH (cell:Cell {ID:toInteger(row.CELL_ID)})
        WHERE toInteger(row.CELL_ID) = cell.ID
        MERGE (call)-[:SOURCE]->(cell)
        RETURN count(row);"""

        create_person_index = "CREATE INDEX person_range_index_number FOR (p:Person) ON (p.number)"
        create_cell_index = "CREATE INDEX cell_range_index_ID FOR (c:Cell) ON (c.ID)"
        create_call_index = "CREATE INDEX call_range_index_ID FOR (c:Call) ON (c.ID)"

        self.create_index_queries = [create_person_index, create_cell_index, create_call_index]

        self.queries = [insert_people, insert_cells, insert_calls, 
                create_caller_called_relationship, create_cells_relationsip]

    def insert_all_data(self, debug=True):
        self.init_queries()

        with self.driver.session() as session:
            if debug: print("Creando gli indici")
            for query in self.create_index_queries:
                session.run(query)
            
            if debug: print("Inserendo i dati")
            for query in self.queries:
                result=session.run(query)
                if debug:
                    print(result.records)