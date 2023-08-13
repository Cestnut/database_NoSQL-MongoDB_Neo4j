from neo4j import GraphDatabase

class NeoInsert:
    def __init__(self, driver):
        self.driver = driver

    def init_queries(self):
        insert = """
        LOAD CSV WITH HEADERS FROM 'file:///{}' AS row
        CALL {{
            WITH row
            MERGE (e:{} {})
        }} IN TRANSACTIONS OF 10000 ROWS RETURN count(*)
        """

        insert_people = insert.format("people.csv", "Person", 
                                    "{number:toInteger(row.NUMBER), first_name:row.FIRST_NAME, last_name:row.LAST_NAME}")
        insert_cells = insert.format("cells.csv", "Cell",
                                    "{ID:toInteger(row.ID), city:row.CITY, address:row.ADDRESS}")

        insert_calls = insert.format("calls.csv", "Call",
                                    "{ID:toInteger(row.ID), begin_timestamp:toInteger(row.BEGIN_TIMESTAMP), end_timestamp:toInteger(row.END_TIMESTAMP)}")


        create_caller_called_relationship = """
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        CALL {
            WITH row
            MATCH (c:Call {ID: toInteger(row.ID)})
            MATCH (p1:Person {number:toInteger(row.CALLER)})
            MATCH (p2:Person {number:toInteger(row.CALLED)})
            WHERE toInteger(row.CALLER) = p1.number AND toInteger(row.CALLED) = p2.number
            MERGE (p1)-[:CALLER]->(c)-[:CALLED]->(p2)
        } IN TRANSACTIONS OF 10000 ROWS RETURN count(*)
        """

        create_cells_relationsip = """
        LOAD CSV WITH HEADERS FROM 'file:///calls.csv' AS row
        CALL {
            WITH row
            MATCH (call:Call {ID: toInteger(row.ID)})
            MATCH (cell:Cell {ID:toInteger(row.CELL_ID)})
            WHERE toInteger(row.CELL_ID) = cell.ID
            MERGE (call)-[:SOURCE]->(cell)
        } IN TRANSACTIONS OF 10000 ROWS RETURN count(*)
        """

        create_person_index = "CREATE INDEX person_range_index_number IF NOT EXISTS FOR (p:Person) ON (p.number)"
        create_cell_index = "CREATE INDEX cell_range_index_ID IF NOT EXISTS FOR (cell:Cell) ON (cell.ID)"
        create_call_index = "CREATE INDEX call_range_index_ID IF NOT EXISTS FOR (call:Call) ON (call.ID)"

        self.delete_query = """
        MATCH (n)
        CALL{
            WITH n
            DETACH DELETE n
        } IN TRANSACTIONS OF 10000 ROWS
        """

        self.create_index_queries = [create_person_index, create_cell_index, create_call_index]

        self.queries = [insert_people, insert_cells, insert_calls, 
                create_caller_called_relationship, create_cells_relationsip]

        

    def clear_database(self, debug):
        with self.driver.session() as session:

            result = session.run(self.delete_query)
            if debug:
                print(result.consume().counters)

    def insert_all_data(self, debug=True):
        self.init_queries()
        self.clear_database(debug)

        with self.driver.session() as session:
            if debug: print("Creando gli indici")
            for query in self.create_index_queries:
                session.run(query)
            
            if debug: print("Inserendo i dati")
            for query in self.queries:
                result=session.run(query)
                if debug:
                    print(result.consume().counters)