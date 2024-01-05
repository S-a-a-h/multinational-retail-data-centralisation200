#This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
#The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.
import pandas as pd
from database_utils import DatabaseConnector

class DataExtractor:
#Initialize an instance via DatabaseConnector:
    def __init__(self):
        self.connector_instance = DatabaseConnector('/Users/itsanya/AiCore/MRDC/db_creds.yaml')

#Take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame
    def read_rds_table(self, table_name):
        engine = self.connector_instance.engine
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        return df
    
    def get_user_data_table(self, connector_instance):
    # Obtain list of tables from DatabaseConnector instance
        tables = connector_instance.list_db_tables()

        user_data_table = None
        for table in tables:
            if 'users' in table.lower() or 'details' in table.lower() or 'tables' in table.lower():
                user_data_table = table
                break

        if user_data_table:
        # Extract table containing user data using read_rds_table method
            return self.read_rds_table(connector_instance, user_data_table)
        else:
            return None 
        
    def read_and_display_table(self, table_name):
        # Extract the DataFrame using read_rds_table method
        data_table = self.read_rds_table(table_name)

        # Display the DataFrame contents
        print(f"Contents of '{table_name}' Table:")
        print(data_table.head()) 





