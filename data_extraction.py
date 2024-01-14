from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self, database_connector):
        self.database_connector = database_connector
    
    def read_rds_table(self, table_name):
        tables = self.database_connector.list_db_tables()
        if table_name in tables:
            engine = self.database_connector.engine
            return pd.read_sql_table(table_name, con=engine)

    def retrieve_pdf_data(self, pdf_link):
        custom_cert_path = '/Applications/IBM/SPSS/Statistics/24/Python3/lib/python3.4/site-packages/pip/_vendor/certifi/cacert.pem'
        response = requests.get(pdf_link, verify=custom_cert_path)
        if response.status_code == 200:
            with open('card_details.pdf', 'wb') as cd:
                cd.write(response.content)
                print(response.status_code)
            df_list = tabula.read_pdf('card_details.pdf', pages='all', stream=True)
            return df_list
        else:
            print(f"Failed to download the PDF. Status code: {response.status_code}")
        
    def list_number_of_stores():
        

    
