
from elasticsearch import Elasticsearch


class DBConnector():
    def __init__(self, config):
        self.type = config['db.type']
        self.db_location = config['db.hostname']
        self.handle = None

    def connect(self):
        if self.type == "elasticsearch":
            ip, port = self.db_location.split(":")
            self.handle = self._es_connect(ip, port)

    def _es_connect(self, ip, port):
        return Elasticsearch([ip])
