import pandas as pd
import tabula
from database_utils import DatabaseConnector

class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector
    
    def read_rds_table(self, table_name):
        tables = self.database_connector.list_db_tables()
        if table_name in tables:
            engine = self.database_connector.engine
            return pd.read_sql_table(table_name, con=engine)

    def retrieve_pdf_data(self, pdf_link):
        pdf_tables = tabula.read_pdf(pdf_link, pages='all', stream=True)
        card_df = pd.concat(pdf_tables, ignore_index=True)
        return card_df