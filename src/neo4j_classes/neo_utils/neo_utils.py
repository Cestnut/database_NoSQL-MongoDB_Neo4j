import time
from neo4j import GraphDatabase

def create_session(driver, timeout):
        start = time.time()
        while time.time() - start <= timeout:
            try:
                driver.verify_connectivity()
                session = driver.session()
                return session
            except:
                time.sleep(2)
        return driver.session()