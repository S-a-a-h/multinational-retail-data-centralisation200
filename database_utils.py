#Connect with and upload data to the database
import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
#initialize the db engine (makes connecting to other classes smoother)
    def __init__(self, creds_path):
        self.creds_path = creds_path
        self.engine = self.init_db_engine()

#Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(self):
        with open(self.creds_path, 'r') as creds:
            credentials = yaml.safe_load(creds)
        return dict(credentials)

#read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self):
        credentials = self.read_db_creds()
        db_url = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

#Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names

#Method that will take in df and table_name to upload to as an argument
    def upload_to_db(self, df, table_name):
        try:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print(f"Data successfully uploaded to '{table_name}'.")
        except Exception as e:
            print(f"Error uploading data to '{table_name}': {str(e)}")

        


