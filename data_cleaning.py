from data_processing import DataProcessor
from IPython.display import display
import pandas as pd

class DataCleaning(DataProcessor):
    
    #STORE_DF
    def clean_store_df(self, store_df):
        cleaned_store_df = DataProcessor.clean_store_type(store_df)
        cleaned_store_df = DataProcessor.clean_store_country_code(cleaned_store_df)
        cleaned_store_df = DataProcessor.clean_store_continent(cleaned_store_df)
        cleaned_store_df = DataProcessor.remove_invalid_values(cleaned_store_df, ['locality', 'store_code'])
        cleaned_store_df = DataProcessor.convert_and_drop_non_numeric(cleaned_store_df, ['latitude', 'staff_numbers', 'longitude'])
        cleaned_store_df = DataProcessor.clean_address(cleaned_store_df, 'address')
        cleaned_store_df = DataProcessor.remove_invalid_dates(cleaned_store_df, ['opening_date'])
        cleaned_store_df = DataProcessor.drop_df_cols(cleaned_store_df, ['lat'])
        cleaned_store_df = DataProcessor.drop_null_values(cleaned_store_df)
        cleaned_store_df = DataProcessor.drop_duplicates(cleaned_store_df)
        cleaned_store_df = DataProcessor.fix_index(cleaned_store_df, 'index')
        return cleaned_store_df

    #USERS_DF
    def clean_users_df(users_df):
        cleaned_users_df = DataProcessor.clean_users_email_address(users_df)
        cleaned_users_df = DataProcessor.clean_users_country(cleaned_users_df)
        cleaned_users_df = DataProcessor.clean_users_country_code(cleaned_users_df)
        cleaned_users_df = DataProcessor.clean_users_company(cleaned_users_df)
        cleaned_users_df = DataProcessor.clean_address(cleaned_users_df, 'address')
        cleaned_users_df = DataProcessor.clean_uuids(cleaned_users_df, ['user_uuid'])
        cleaned_users_df = DataProcessor.remove_invalid_dates(cleaned_users_df, ['join_date', 'date_of_birth'])
        cleaned_users_df = DataProcessor.clean_fnames_lnames(cleaned_users_df)
        cleaned_users_df = DataProcessor.drop_null_values(cleaned_users_df)
        cleaned_users_df = DataProcessor.drop_duplicates(cleaned_users_df)
        cleaned_users_df = DataProcessor.fix_index(cleaned_users_df, 'index')
        return cleaned_users_df

    #ORDERS_DF
    def clean_orders_df(self, orders_df):
        cleaned_orders_df = DataProcessor.clean_orders_store_code(orders_df)
        cleaned_orders_df = DataProcessor.convert_and_drop_non_numeric(cleaned_orders_df, ['product_quantity'])
        cleaned_orders_df = DataProcessor.clean_fnames_lnames(cleaned_orders_df)
        cleaned_orders_df = DataProcessor.clean_uuids(cleaned_orders_df, ['user_uuid', 'date_uuid'])       
        cleaned_orders_df = DataProcessor.drop_df_cols(cleaned_orders_df,  ['level_0', '1'])
        cleaned_orders_df = DataProcessor.drop_null_values(cleaned_orders_df)
        cleaned_orders_df = DataProcessor.drop_duplicates(cleaned_orders_df)
        cleaned_orders_df = DataProcessor.fix_index(cleaned_orders_df, 'index')
        return cleaned_orders_df
        

        

