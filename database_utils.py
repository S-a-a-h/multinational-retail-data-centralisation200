#Connect with and upload data to the database
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
#Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self, creds_path):
        with open(creds_path, 'r') as creds:
            credentials = yaml.safe_load(creds)
        return dict(credentials)


#read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self, creds_path):
        credentials = self.read_db_creds(creds_path)
        db_url = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

#Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
    def list_db_tables(self, creds_path):
        engine = self.init_db_engine(creds_path)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
        


