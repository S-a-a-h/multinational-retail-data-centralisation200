#Connect with and upload data to the database
import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    def __init__(self, creds_path):
        self.creds_path = creds_path
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open(self.creds_path, 'r') as creds:
            credentials = yaml.safe_load(creds)
        return dict(credentials)
    
    def init_db_engine(self):
        credentials = self.read_db_creds()
        db_url = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine    

    def list_db_tables(self):
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        return table_names   

    def upload_to_db(self, table_name, df, index=False, if_exists='replace'):
        with open('/Users/itsanya/AiCore/MRDC/sales_data_creds.yaml', 'r') as ldbcreds:
            ldb_creds = yaml.safe_load(ldbcreds)

        ldb_connect = f"postgresql://{ldb_creds['username']}:{ldb_creds['password']}@{ldb_creds['host']}:{ldb_creds['port']}/{ldb_creds['database']}"
        ldb_engine = create_engine(ldb_connect)

        df.to_sql(name=table_name, con=ldb_engine, index=index, if_exists=if_exists)
        print(f"Data uploaded to '{table_name}' table successfully.")
