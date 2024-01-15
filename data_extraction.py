from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import json

class DataExtractor:
    def __init__(self, db_connector_instance):
        self.db_connector_instance = db_connector_instance
        self.header = None  #remeber to change header when using api-key


    #USERS_DF
    def read_rds_table(self, table_name):
        tables = self.database_connector.list_db_tables()
        if table_name in tables:
            engine = self.database_connector.engine
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
        number_of_stores = response.json().get('count')
        if response.status_code == 200:
            number_of_stores = response.json().get('number_stores')
            print(f"Number of stores: {number_of_stores}")
        return number_of_stores
    
    #def retrieve_stores_data(self, store_endpoint):
    #    response = requests.get(store_endpoint, headers=self.header)
    #    b_store_data = response.json().get('stores')
    #    if b_store_data:
    #        df = pd.DataFrame(b_store_data)
    #        return df

            

    def retrieve_stores_data(self, store_endpoint):
        response = requests.get(store_endpoint, headers=self.header)

        try:
            json_content = response.json()

            if isinstance(json_content, (dict, list)):
                b_store_data = json_content.get('stores')
                if b_store_data:
                    df = pd.DataFrame(b_store_data)
                    return df
                else:
                    print("No store data found.")
                    return None
            else:
                # Handle non-JSON content
                print(f"Non-JSON content received: {json_content}")  #output - server issue? also had status 500 error with other code!
                return None
        except json.JSONDecodeError:
            # Handle JSON decoding error
            print(f"Error decoding JSON content.")
            print(f"Raw content: {response.text}")  # Print the raw content for debugging

            # Assume it's non-JSON content and handle it accordingly
            print("Assuming non-JSON content. Handling it...")

            # Your additional handling logic for non-JSON content
            if "error" in response.text.lower():
                print("Server returned an error message.")
                # Additional handling based on the specific error format
                # ...

            return None

