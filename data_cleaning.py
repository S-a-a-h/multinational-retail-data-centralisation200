#Class with methods to clean data from each of the data sources
import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

class DataCleaning:
#Initialize:
    def __init__ (self, creds_path):
        self.connector_instance = DatabaseConnector(creds_path)
        self.extractor_instance = DataExtractor()

#Clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information

#Handling NULL values: Replace NULLs with a default value, impute using statistics, or drop rows/columns with too many missing values.
#Correcting date errors: Standardize date formats, parse and fill missing or incorrect dates.
#Correcting incorrectly typed values: Convert data types to the appropriate format (e.g., dates to datetime objects, strings to numeric values).
#Handling rows with wrong information: Identify outliers or incorrect entries and remove or correct them.
    def clean_user_data(self):
        

#This method will take in a Pandas DataFrame and table name to upload to as an argument.
    def upload_to_db():
