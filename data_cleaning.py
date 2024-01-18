from data_processing import DataProcessor


class DataCleaning(DataProcessor):
    '''
    DataCleaning Class
    -------
    This script cleans the data via the inherited class: DataProcessor.
    The methods included are all staticmethods for code reusablity on all DataFrames created in this project.
    All methods are named descriptively for readability and all variables are distinct for each DataFrame for debugging purposes.

    '''

    #USERS_DF (users_df)
    def clean_users_df(self, users_df):
        cleaned_users_df_dup = DataProcessor.drop_duplicates(users_df) 
        cleaned_users_df_c_code = DataProcessor.clean_users_country_code(cleaned_users_df_dup) 
        cleaned_users_df_add = DataProcessor.clean_address(cleaned_users_df_c_code, 'address') 
        cleaned_users_df_uuid = DataProcessor.clean_uuids(cleaned_users_df_add, ['user_uuid']) #PK
        cleaned_users_df_dates = DataProcessor.clean_dates(cleaned_users_df_uuid, ['join_date', 'date_of_birth'])
        cleaned_users_df = DataProcessor.fix_index(cleaned_users_df_dates, 'index')
        return cleaned_users_df
    
    #CARD_DF (card_df)
    def clean_card_data(self, card_df): 
        cleaned_card_df_dup = DataProcessor.drop_duplicates(card_df) 
        cleaned_card_df_dropcols = DataProcessor.drop_df_cols(cleaned_card_df_dup, ['card_number expiry_date', 'Unnamed: 0']) 
        cleaned_card_df_pdates = DataProcessor.clean_dates(cleaned_card_df_dropcols, ['date_payment_confirmed']) 
        cleaned_card_df_edates = DataProcessor.format_expiry_dates(cleaned_card_df_pdates) 
        cleaned_card_df = DataProcessor.clean_card_number(cleaned_card_df_edates, 'card_number') #PK
        return cleaned_card_df
    
    #BUSINESS STORE DF (b_store_df)
    def clean_store_data(self, b_store_df): 
        cleaned_b_store_df_dup = DataProcessor.drop_duplicates(b_store_df) 
        cleaned_b_store_df_con = DataProcessor.clean_store_continent(cleaned_b_store_df_dup) 
        cleaned_b_store_df_s_c = DataProcessor.clean_store_code(cleaned_b_store_df_con) #PK
        cleaned_b_store_df_drop = DataProcessor.drop_df_cols(cleaned_b_store_df_s_c, ['lat']) 
        cleaned_b_store_df_num = DataProcessor.tonumeric(cleaned_b_store_df_drop, ['longitude', 'staff_numbers', 'latitude']) 
        cleaned_b_store_df_add = DataProcessor.clean_address(cleaned_b_store_df_num, 'address') 
        cleaned_b_store_df_odate = DataProcessor.clean_dates(cleaned_b_store_df_add, ['opening_date']) 
        cleaned_b_store_df = DataProcessor.fix_index(cleaned_b_store_df_odate, 'index') 
        return cleaned_b_store_df
    
    #PRODUCTS DF (prods_df)
    def convert_product_weights(self, prods_df):
        prods_df = DataProcessor.process_prod_weight(prods_df) 
        return prods_df
    
    def clean_products_data(self, prods_df):
        cleaned_prods_df_dup = DataProcessor.drop_duplicates(prods_df) 
        cleaned_prods_df_col_n = DataProcessor.clean_col_names(cleaned_prods_df_dup) 
        cleaned_prods_df_pc = DataProcessor.clean_prod_codes(cleaned_prods_df_col_n) #PK
        cleaned_prods_df_date = DataProcessor.clean_dates(cleaned_prods_df_pc, ['date_added']) 
        cleaned_prods_df = DataProcessor.fix_index(cleaned_prods_df_date, 'index') 
        return cleaned_prods_df

    #ORDERS_DF (orders_df)
    def clean_orders_df(self, orders_df):
        cleaned_orders_df_dup = DataProcessor.drop_duplicates(orders_df) 
        cleaned_orders_df_drop_cols = DataProcessor.drop_df_cols(cleaned_orders_df_dup, ['level_0', 'first_name', 'last_name', '1']) 
        cleaned_orders_df_uuids = DataProcessor.clean_uuids(cleaned_orders_df_drop_cols, ['date_uuid', 'user_uuid']) 
        cleaned_orders_df_cnum = DataProcessor.clean_card_number(cleaned_orders_df_uuids, 'card_number') 
        cleaned_orders_df = DataProcessor.fix_index(cleaned_orders_df_cnum, 'index')
        return cleaned_orders_df

    #SALES DATE TIMES (sdt_df)
    def clean_sdt_df(self, sdt_df):
        cleaned_sdt_df_dup = DataProcessor.drop_duplicates(sdt_df) 
        cleaned_sdt_df_time = DataProcessor.clean_timestamp(cleaned_sdt_df_dup) 
        cleaned_sdt_df = DataProcessor.clean_uuids(cleaned_sdt_df_time, ['date_uuid']) #PK
        return cleaned_sdt_df
        

