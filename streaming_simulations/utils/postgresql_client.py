import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class PostgresSQLClient:
    def __init__(self, database, user, password, host="127.0.0.1", port="5434"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
    
    def create_conn(self):
        conn = psycopg2.connect(
            database = self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )

        return conn
    
    def execute_query(self, query):
        conn = self.create_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        print(f"A new record has been inserted successfully!")
        conn.commit()
        conn.close()
    
    def get_colums(self, table_name):
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}"
            )
            conn = engine.connect()
            df = pd.read_sql(f"select * from {table_name}", conn)
        except Exception as e:
            print(f"failed in get columns with error: {e}")
        return df.columns