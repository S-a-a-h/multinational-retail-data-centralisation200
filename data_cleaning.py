#Class with methods to clean data from each of the data sources
import pandas as pd
from data_transform import DataTransformer


class DataCleaning:
    #DataTransformer.convert_col_dtype(column="Name", type="str")
    #DataTransformer.clean_weights(column_name="weights") 
    #syntax: class.cleaning_method_from_class(column_to_clean_from_df)

    @staticmethod
    def clean_store_data(legacy_store_details):
        #legacy_store_details['country_code'].unique()
        legacy_store_details = legacy_store_details.drop(legacy_store_details[~legacy_store_details['country_code'].isin(['GB', 'US', 'DE'])].index)
        return legacy_store_details


    @staticmethod
    def hello():
        print('hello world')

    from data_transform import DataTransformer

transform = DataTransformer()
columns_to_process = ['latitude', 'staff_numbers', 'longitude']
store_df = transform.convert_and_drop(store_df, columns_to_process)

    #def clean_users_data(legacy_users_df):




    #def clean_orders_data(orders_table_df):
        

        

