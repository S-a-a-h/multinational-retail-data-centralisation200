#Connect with and upload data to the database
import yaml

class DatabaseConnector:
#Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
    def read_db_creds(file_path):
        with open(file_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials
    
        file_path = '/Users/itsanya/AiCore/MRDC/db_creds.yaml'
        credentials = read_db_creds(file_path)
        return dict(credentials)


#read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine():
        credentials = read_db_creds()



#Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
    def list_db_tables():
            


