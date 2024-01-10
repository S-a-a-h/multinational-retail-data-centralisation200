from data_processing import DataProcessor
import pandas as pd

class DataCleaning(DataProcessor):
    
    #STORE_DF
    def clean_store_df(store_df):
        cleaned_store_df = DataProcessor.clean_store_type(store_df)
        cleaned_store_df = DataProcessor.clean_store_country_code(store_df)
        cleaned_store_df = DataProcessor.clean_store_continent(store_df)
        cleaned_store_df = DataProcessor.remove_invalid_values(store_df, ['locality', 'store_code'])
        cleaned_store_df = DataProcessor.convert_and_drop_non_numeric(store_df, ['latitude', 'staff_numbers', 'longitude'])
        cleaned_store_df = DataProcessor.clean_address(store_df, 'address')
        cleaned_store_df = DataProcessor.drop_df_cols(store_df, ['lat'])
        cleaned_store_df = DataProcessor.remove_invalid_dates(store_df, ['opening_date'])
        cleaned_store_df = DataProcessor.drop_duplicates(store_df)
        cleaned_store_df = DataProcessor.fix_index(store_df, 'index')
        return cleaned_store_df

    #USERS_DF
    def clean_users_df(users_df):
        cleaned_users_df = DataProcessor.clean_users_email_address(users_df)
        cleaned_users_df = DataProcessor.clean_users_country(users_df)
        cleaned_users_df = DataProcessor.clean_users_country_code(users_df)
        cleaned_users_df = DataProcessor.clean_users_company(users_df)
        cleaned_users_df = DataProcessor.clean_address(users_df, 'address')
        cleaned_users_df = DataProcessor.clean_uuids(users_df, ['user_uuid'])
        cleaned_users_df = DataProcessor.remove_invalid_dates(users_df, ['join_date', 'date_of_birth'])
        cleaned_users_df = DataProcessor.clean_fnames_lnames(users_df)
        cleaned_users_df = DataProcessor.drop_duplicates(users_df)
        cleaned_users_df = DataProcessor.fix_index(users_df, 'index')
        return cleaned_users_df

    #ORDERS_DF
    def clean_orders_df(orders_df):
        cleaned_orders_df = DataProcessor.clean_orders_store_code(orders_df)
        cleaned_orders_df = DataProcessor.convert_and_drop_non_numeric(orders_df, ['product_quantity'])
        cleaned_orders_df = DataProcessor.clean_fnames_lnames(orders_df)
        cleaned_orders_df = DataProcessor.clean_uuids(orders_df, ['user_uuid', 'date_uuid'])       
        cleaned_orders_df = DataProcessor.drop_df_cols(orders_df,  ['level_0', '1'])
        cleaned_orders_df = DataProcessor.drop_duplicates(orders_df)
        cleaned_orders_df = DataProcessor.fix_index(orders_df, 'index')
        return cleaned_orders_df
        

        

