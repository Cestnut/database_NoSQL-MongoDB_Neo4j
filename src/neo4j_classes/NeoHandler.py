from neo4j_classes.NeoInsert import NeoInsert
from neo4j_classes.NeoRead import NeoRead
from neo4j import GraphDatabase
from .neo_utils import neo_utils
class NeoHandler:
    def __init__(self, begin, end, city, host="localhost", port=7687, timeout=60):
        
        self.driver = GraphDatabase.driver("bolt://{}:{}".format(host,port))
        self.session_timeout = timeout

        self.inserter = NeoInsert(self.driver, timeout)
        self.reader = NeoRead(begin, end, city, self.driver, self.session_timeout)

        #Per quanto tempo provare a creare una sessione prima di restituire un errore

    def clear_cache(self):
        session = neo_utils.create_session(self.driver, self.session_timeout)
        session.run("CALL db.clearQueryCaches()")
        session.close()