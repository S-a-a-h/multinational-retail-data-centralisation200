from sqlalchemy import create_engine, inspect
import yaml


class DatabaseConnector:

    '''
    DataConnector Class
    -------
    This script creates the engine for the connection to the database and uploads the DataFrames to the database once data is transformed.
    The methods included are mostly public methods for code reusablity.

    Methods
    -------
        * __init__ - a magic method which initializes the engine created and path to the credentials file
        * _read_db_creds - a private method to read in the credentials associated with creating the engine
        * _init_db_engine - a private method which initializes the engine with the relvant credentials 
        * list_db_tables - a public method which retrieves only the table names from an RDS database
        * _get_local_db_engine - a private method which specifies the local database credentials path to create the engine to upload the transformed data to the database
        * _upload_to_db - a private method which uploads transformed data to sales_data database and notifies user of upload success
    '''
        
    def __init__(self, creds_path):
        self.creds_path = creds_path
        self.engine = self.init_db_engine()


    def _read_db_creds(self):
        with open(self.creds_path, 'r') as creds:
            credentials = yaml.safe_load(creds)
        return dict(credentials)
    
    
    def _init_db_engine(self):
        credentials = self.read_db_creds()
        db_url = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine    


    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names   
    

    #UPLOAD TO DB
    def _get_local_db_engine(self):
        with open('/Users/itsanya/AiCore/MRDC/sales_data_creds.yaml', 'r') as ldbcreds:
            ldb_creds = yaml.safe_load(ldbcreds)

        ldb_connect = f"postgresql://{ldb_creds['username']}:{ldb_creds['password']}@{ldb_creds['host']}:{ldb_creds['port']}/{ldb_creds['database']}"
        return create_engine(ldb_connect)

    def _upload_to_db(self, df, table_name, index=False, if_exists='replace'):
        ldb_engine = self.get_local_db_engine()
        df.to_sql(name=table_name, con=ldb_engine, index=index, if_exists=if_exists)
        print(f"Data uploaded to '{table_name}' table successfully.")
