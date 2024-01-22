from database_utils import DatabaseConnector
import boto3
import json
import pandas as pd
import requests
import tabula


class DataExtractor:
    
    '''
    DataExtractor Class
    -------
    This script extracts the raw data from their respective sources. 
    The methods included are mostly public methods for code reusablity in further data extraction from varying sources.

    Methods
    -------
        * __init__ - a magic method which uses an instance of class DatabaseConnecter and initializes it, applies the header attribute for api-key use and utilizes the aws cli
        * read_rds_table - a public method to read in tables from a relational database using the engine created in class DatabaseConnector
        * _retrieve_pdf_data - a private method which retrieves the card data from a pdf link and is coded to bypass an SSLCertification Error by instead downloading the file from the pdf_link and assigning a cacert.pem file to overcome the error. 
        * set_api_key - a public method which alters the header attribute by assigning the api-keys
        * list_number_of_stores - a public method that returns the number of stores as an interger
        * retrieve_stores_data - a public method that retrieves the data, sorts it and returns a single pandas DataFrame
        * extract_store_data - a public method that organizes the columns of the DataFrame returned by retrieve_stores_data
        * extract_from_s3 - a public method that extracts data from an Amazon S3 Bucket via the bucket address
        * extract_sdt - a public method that extracts data from an S3 Bucket using the link to the Amazon S3 Bucket
    '''

    def __init__(self, db_connector_instance):
        self.db_connector_instance = db_connector_instance
        self.header = None #assign header when in use
        self.s3_client = boto3.client('s3')


    #USERS_DF & ORDERS_DF
    def read_rds_table(self, table_name):
        tables = self.db_connector_instance.list_db_tables()
        if table_name in tables:
            engine = self.db_connector_instance.engine
            return pd.read_sql_table(table_name, con=engine)


    #CARD_DF
    def _retrieve_pdf_data(self, pdf_link):
        custom_cert_path = '/Applications/IBM/SPSS/Statistics/24/Python3/lib/python3.4/site-packages/pip/_vendor/certifi/cacert.pem'
        response = requests.get(pdf_link, verify=custom_cert_path)
        if response.status_code == 200:
            with open('card_details.pdf', 'wb') as cd:
                cd.write(response.content)
            df_list = tabula.read_pdf('card_details.pdf', pages='all', stream=True)
            combined_df = pd.concat(df_list, ignore_index=True)
            return combined_df
    

    #B_STORE_DF
    def set_api_key(self, b_store_api_key):
        self.header = {'x-api-key': b_store_api_key}

    def list_number_of_stores(self, number_of_stores_endpoint):
        response = requests.get(number_of_stores_endpoint, headers=self.header)
        number_of_stores = response.json().get('number_stores')
        print(f'Number of stores: {number_of_stores}')
        return int(number_of_stores)

    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        store_dfs = []

        for store_number in range(number_of_stores):
            endpoint = store_endpoint.format(store_number=store_number)
            response = requests.get(endpoint, headers=self.header)
            store_data = response.json()

            extracted_store_data = self.extract_store_data(store_data)
            store_df = pd.DataFrame(extracted_store_data, index=[store_data.get('index')])
            store_dfs.append(store_df)

        all_stores_df = pd.concat(store_dfs)
        return all_stores_df
    
    def extract_store_data(self, store_data):
        return {
            'index': store_data.get('index'),
            'address': store_data.get('address'),
            'longitude': store_data.get('longitude'),
            'lat': store_data.get('lat'),
            'locality': store_data.get('locality'),
            'store_code': store_data.get('store_code'),
            'staff_numbers': store_data.get('staff_numbers'),
            'opening_date': store_data.get('opening_date'),
            'store_type': store_data.get('store_type'),
            'latitude': store_data.get('latitude'),
            'country_code': store_data.get('country_code'),
            'continent': store_data.get('continent')
        }


    #PRODS_DF
    def extract_from_s3(self, s3_address):
        bucket, key = s3_address.split('://')[1].split('/', 1)
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        data = pd.read_csv(response['Body'])
        prods_df = pd.DataFrame(data)
        return prods_df


    #SDT_DF
    def extract_sdt(self, s3_url):
        response = requests.get(s3_url)
        sdt = json.loads(response.text)
        sdt_df = pd.DataFrame(sdt)
        return sdt_df


