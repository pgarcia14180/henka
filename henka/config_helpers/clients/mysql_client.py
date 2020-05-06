import mysql.connector
from henka.utils.dictionary import DictClass, trim_dictionary

class MySqlClient(DictClass):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = self.get_client()
        if "table_name" in self:
            self.set_column_types()

    def get_client(self):
        params = trim_dictionary(self, "host", "user", "passwd", "database")
        return mysql.connector.connect(**params)

    def execute_query(self):
        cursor = self.client.cursor(buffered=True)
        for i, q in enumerate(self.query.split(";")):
            if q.strip():
                cursor.execute(q+";")
        self.cursor = cursor
        return cursor

    def set_column_types(self):
        self.column_types = {}
        query = None
        if "query" in self:
            query = self.query
        self.query = f"show fields from {self.table_name}"
        self.execute_query()
        fetched_query = self.fetch_query()
        for field in fetched_query:
            if field: self.column_types[field[0]] = field[1]
        self.query = query

    def commit(self):
        cursor = self.cursor
        self.client.commit()
        print(cursor.rowcount, "record(s) affected")

    def get_column_names(self):
        if " select " in self.query or self.query.startswith("select "):
            return [i[0] for i in self.cursor.description]

    def fetch_query(self):
        return self.cursor.fetchall()
