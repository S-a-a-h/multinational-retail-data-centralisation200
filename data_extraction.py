import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector
    
    def read_rds_table(self, table_name):
        tables = self.database_connector.list_db_tables()
        if table_name in tables:
            engine = self.database_connector.engine
            return pd.read_sql_table(table_name, con=engine)
