from neo4j_classes.NeoInsert import NeoInsert
from neo4j_classes.NeoRead import NeoRead
from neo4j import GraphDatabase

class NeoHandler:
    def __init__(self, begin, end, city, host="localhost", port=7687):
        
        self.database = GraphDatabase.driver("bolt://{}:{}".format(host,port))

        self.inserter = NeoInsert(self.database)
        self.reader = NeoRead(begin, end, city, self.database)

    def clear_cache(self):
        with self.database.session() as session:
            session.run("CALL db.clearQueryCaches()")
