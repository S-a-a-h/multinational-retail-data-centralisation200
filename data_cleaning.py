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
        cleaned_card_df = DataProcessor.clean_card_number(cleaned_card_df_edates) #formats and drops
        return cleaned_card_df
    
    #BUSINESS STORE DF (b_store_df)
    def clean_store_data(self, b_store_df): 
        cleaned_b_store_df_dup = DataProcessor.drop_duplicates(b_store_df)
        cleaned_b_store_df_add = DataProcessor.clean_address(cleaned_b_store_df_dup, 'address')
        cleaned_b_store_df_con = DataProcessor.clean_store_continent(cleaned_b_store_df_add) 
        cleaned_b_store_df_drop = DataProcessor.drop_df_cols(cleaned_b_store_df_con, ['lat'])
        cleaned_b_store_df_num = DataProcessor.tonumeric_and_drop_non_numeric(cleaned_b_store_df_drop, ['longitude', 'staff_numbers', 'latitude']) 
        cleaned_b_store_df_odate = DataProcessor.clean_dates(cleaned_b_store_df_num, ['opening_date']) 
        cleaned_b_store_df = DataProcessor.fix_index(cleaned_b_store_df_odate, 'index') 
        return cleaned_b_store_df



        

