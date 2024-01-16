from data_processing import DataProcessor
import pandas as pd

class DataCleaning(DataProcessor):
    
    #USERS_DF (users_df)
    def clean_users_df(self, users_df):
        cleaned_users_df_dup = DataProcessor.drop_duplicates(users_df) #drops
        cleaned_users_df_con = DataProcessor.clean_users_country(cleaned_users_df_dup) #filters and drops
        cleaned_users_df_c_code = DataProcessor.clean_users_country_code(cleaned_users_df_con) #filters and drops
        cleaned_users_df_address = DataProcessor.clean_address(cleaned_users_df_c_code, 'address') #filters only
        cleaned_users_df_uuid = DataProcessor.clean_uuids(cleaned_users_df_address, ['user_uuid']) #drops
        cleaned_users_df_dates = DataProcessor.clean_dates(cleaned_users_df_uuid, ['join_date', 'date_of_birth']) #drops
        cleaned_users_df = DataProcessor.fix_index(cleaned_users_df_dates, 'index')
        return cleaned_users_df
    
    #CARD_DF (card_df)
    def clean_card_data(self, card_df): 
        cleaned_card_df_dup = DataProcessor.drop_duplicates(card_df) #drops
        cleaned_card_df_cprov = DataProcessor.card_prov(cleaned_card_df_dup) #filters only
        cleaned_card_df_dropcols = DataProcessor.drop_df_cols(cleaned_card_df_cprov, ['card_number expiry_date', 'Unnamed: 0']) #drops
        cleaned_card_df_pdates = DataProcessor.clean_dates(cleaned_card_df_dropcols, ['date_payment_confirmed']) #formats and drops
        cleaned_card_df_edates = DataProcessor.clean_store_edate(cleaned_card_df_pdates) #formats and drops
        cleaned_card_df = DataProcessor.clean_card_number(cleaned_card_df_edates, 'card_number') #formats and drops
        return cleaned_card_df
    
    #BUSINESS STORE DF (b_store_df)
    def clean_store_data(self, b_store_df): 
        cleaned_b_store_df_dup = DataProcessor.drop_duplicates(b_store_df) #drops
        cleaned_b_store_df_con = DataProcessor.clean_store_continent(cleaned_b_store_df_dup) #filters only
        cleaned_b_store_df_drop = DataProcessor.drop_df_cols(cleaned_b_store_df_con, ['lat']) #drops
        cleaned_b_store_df_num = DataProcessor.tonumeric_and_drop_non_numeric(cleaned_b_store_df_drop, ['longitude', 'staff_numbers', 'latitude']) #formats and drops
        cleaned_b_store_df_odate = DataProcessor.clean_dates(cleaned_b_store_df_num, ['opening_date']) #formats and drops
        cleaned_b_store_df = DataProcessor.fix_index(cleaned_b_store_df_odate, 'index') 
        return cleaned_b_store_df
    
    #PRODUCTS DF (prods_df)
    def convert_product_weights(self, prods_df):
        prods_df = DataProcessor.process_prod_weight(prods_df) #formats only
        return prods_df
    
    def clean_products_data(self, prods_df):
        cleaned_prods_df_dup = DataProcessor.drop_duplicates(prods_df) #drops
        cleaned_prods_df_col_n = DataProcessor.clean_col_names(cleaned_prods_df_dup) #changes col names
        cleaned_prods_df_uuid = DataProcessor.clean_uuids(cleaned_prods_df_col_n, ['uuid']) #filters only
        cleaned_prods_df_ean = DataProcessor.clean_EAN(cleaned_prods_df_uuid) #filters and drops
        cleaned_prods_df_date = DataProcessor.clean_dates(cleaned_prods_df_ean, ['date_added']) #formats and drops
        cleaned_prods_df = DataProcessor.fix_index(cleaned_prods_df_date, 'index') 
        return cleaned_prods_df

    #ORDERS_DF (orders_df)
    def clean_orders_df(self, orders_df):
        cleaned_orders_df_dup = DataProcessor.drop_duplicates(orders_df) #drops
        cleaned_orders_df_drop_cols = DataProcessor.drop_df_cols(cleaned_orders_df_dup, ['level_0', 'first_name', 'last_name', '1']) #drops
        cleaned_orders_df_uuids = DataProcessor.clean_uuids(cleaned_orders_df_drop_cols, ['date_uuid', 'user_uuid']) #filters only
        cleaned_orders_df_cnum = DataProcessor.clean_card_number(cleaned_orders_df_uuids, 'card_number') #formats and drops
        cleaned_orders_df_prodq = DataProcessor.tonumeric_and_drop_non_numeric(cleaned_orders_df_cnum, ['product_quantity']) #formats and drops
        cleaned_orders_df = DataProcessor.fix_index(cleaned_orders_df_prodq, 'index')
        return cleaned_orders_df

    #SALES DATE TIMES (sdt_df)
    def clean_sdt_df(self, sdt_df):
        cleaned_sdt_df_dup = DataProcessor.drop_duplicates(sdt_df) #drops
        cleaned_sdt_df_drop_tp = DataProcessor.clean_time_period(cleaned_sdt_df_dup) #filters only
        cleaned_sdt_df_time = DataProcessor.clean_timestamp(cleaned_sdt_df_drop_tp ) #formats and drops
        cleaned_sdt_df_uuid = DataProcessor.clean_uuids(cleaned_sdt_df_time, ['date_uuid']) #formats and drops
        cleaned_sdt_df = DataProcessor.tonumeric_and_drop_non_numeric(cleaned_sdt_df_uuid, ['month', 'year', 'day']) #formats and drops
        return cleaned_sdt_df
        

