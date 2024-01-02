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
    





