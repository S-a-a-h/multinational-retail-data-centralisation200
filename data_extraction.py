from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import boto3
import json

class DataExtractor:
    def __init__(self, db_connector_instance):
        self.db_connector_instance = db_connector_instance
        self.header = None  #change header when using api-key in main code execution
        self.s3_client = boto3.client('s3')


    #USERS_DF & ORDERS_DF
    def read_rds_table(self, table_name):
        tables = self.db_connector_instance.list_db_tables()
        if table_name in tables:
            engine = self.db_connector_instance.engine
            return pd.read_sql_table(table_name, con=engine)


    #CARD_DF
    def retrieve_pdf_data(self, pdf_link):
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
        if response.status_code == 200:
            number_of_stores = response.json().get('number_stores')
            print(f'Number of stores: {number_of_stores}')
        return int(number_of_stores)

    def retrieve_stores_data(self, store_endpoint, number_of_stores):
        store_dfs = []

        for store_number in range(number_of_stores):
            endpoint = store_endpoint.format(store_number=store_number)
            response = requests.get(endpoint, headers=self.header)
            store_data = response.json()

            extracted_data = {
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

            store_df = pd.DataFrame(extracted_data, index=[store_data.get('index')])
            store_dfs.append(store_df)

        all_stores_df = pd.concat(store_dfs)
        return all_stores_df

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
        if response.status_code == 200:
            sdt = json.loads(response.text)
            print('File downloaded successfully.')
            sdt_df = pd.DataFrame(sdt)
            return sdt_df


