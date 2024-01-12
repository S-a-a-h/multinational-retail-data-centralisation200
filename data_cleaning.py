from data_processing import DataProcessor

class DataCleaning(DataProcessor):
    
    #USERS_DF
    def clean_users_df(self, users_df):
        cleaned_users_df_dup = DataProcessor.drop_duplicates(users_df) #drops
        cleaned_users_df_con = DataProcessor.clean_users_country(cleaned_users_df_dup) #filters and drops
        cleaned_users_df_c_code = DataProcessor.clean_users_country_code(cleaned_users_df_con) #filters and drops
        cleaned_users_df_address = DataProcessor.clean_address(cleaned_users_df_c_code, 'address') #filters only
        cleaned_users_df_uuid = DataProcessor.clean_uuids(cleaned_users_df_address, ['user_uuid']) #drops
        cleaned_users_df_dates = DataProcessor.clean_dates(cleaned_users_df_uuid, ['join_date', 'date_of_birth']) #drops
        cleaned_users_df = DataProcessor.fix_index(cleaned_users_df_dates, 'index')
        return cleaned_users_df
    
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
        

        

