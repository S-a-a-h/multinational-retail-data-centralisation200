import pandas as pd
import numpy as np
import re


class DataProcessor:
    '''
    DataProcessor Class
    -------
    This script transforms the data by processing, correcting values and dropping any invalid rows from each DataFrame.
    All methods and variables are named descriptively for readability.
    '''

    #USERS_DF METHODS ONLY 

    def amend_users_country_code(users_df):
        valid_country_codes = ['GB', 'US', 'DE']
        country_code_mapping = {'GGB': 'GB'}
        users_df.loc[:, 'country_code'] = users_df['country_code'].apply(lambda c_c: country_code_mapping.get(c_c, c_c))
        users_df = users_df[users_df['country_code'].isin(valid_country_codes)]
        return users_df


    #CARD_DF METHODS ONLY

    def filter_card_number(card_df):
        card_df['card_number'] = card_df['card_number'].astype(str)
        card_df['card_number'] = card_df['card_number'].str.replace('?', '')
        card_df['card_number'] = card_df['card_number'].str.replace('.', '')
        card_df['card_number'] = card_df['card_number'].str.replace(r'\.0$', '')
        return card_df

    def format_expiry_dates(card_df):
        card_df['expiry_date'] = card_df['expiry_date'].apply(lambda x: pd.to_datetime(x, format='%m/%y', errors='coerce')).dt.date 
        return card_df


    #B_STORE_DF METHODS ONLY 

    def amend_store_continent(b_store_df):
        b_store_df['continent'] = b_store_df['continent'].apply(lambda con: con[2:] if con and con.startswith('ee') else con)
        return b_store_df

    def convert_to_numeric(b_store_df, column_names):
        for column_name in column_names:
            b_store_df[column_name] = pd.to_numeric(b_store_df[column_name], errors='coerce')
        return b_store_df
    
    def filter_store_code(b_store_df):
        pattern = r'^[A-Z]{2}-\w{8}$|^WEB-1388012W$'
        mask = b_store_df['store_code'].str.match(pattern, na=False)
        b_store_df = b_store_df[mask]
        return b_store_df


    #PRODS_DF METHODS ONLY

    def convert_weight(weight):
        if pd.isna(weight):
            return weight

        if 'g' in weight or 'ml' in weight:
            numeric_value = pd.to_numeric(''.join(char for char in weight if char.isdigit() or char == '.'), errors='coerce') / 1000
            return numeric_value if not pd.isna(numeric_value) else weight
        elif 'oz' in weight:

            numeric_value = pd.to_numeric(''.join(char for char in weight if char.isdigit() or char == '.'), errors='coerce') * 0.0283495
            return numeric_value if not pd.isna(numeric_value) else weight
        else:
            return weight

    def process_prod_weight(prods_df):
        prods_df['weight'] = prods_df['weight'].str.replace('kg', '')
        prods_df['weight'] = prods_df['weight'].apply(DataProcessor.convert_weight)
        return prods_df
    
    def change_column_names(prods_df):
        prods_df = prods_df.rename(columns={'Unnamed: 0': 'index'})
        prods_df = prods_df.rename(columns={'weight': 'weight(kg)'})
        prods_df = prods_df.rename(columns={'removed': 'product_status'})
        return prods_df
    
    def filter_product_code(prods_df):
        hyphen_mask = prods_df['product_code'].str.contains('-', na=False)
        prods_df_pc_with_hyphen = prods_df[hyphen_mask].copy()
        return prods_df_pc_with_hyphen
    

    #SDT_DF METHODS ONLY
 
    def standardize_timestamp(sdt_df):
        sdt_df['timestamp'] = pd.to_datetime(sdt_df['timestamp'], errors='coerce').dt.time
        return sdt_df


    #METHODS APPLICABLE TO MORE THAN 1 DF 

    def filter_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        mask = df[column_names].apply(lambda uuid: uuid.astype(str).str.match(uuid_pattern, na=False)).all(axis=1)
        df = df[mask]
        return df

    def standardize_address(df, column_name):
        df[column_name] = df[column_name].apply(lambda address: str(address).replace('\n', ' ') if pd.notna(address) else address)
        return df  

    def standardize_date_column(df, column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='mixed', errors='coerce')
        return df

    def drop_df_column(df, column_names, axis=1):
        df.drop(columns=column_names, inplace=True)
        return df

    def fix_index(df, index_col):
        df.reset_index(drop=True, inplace=True)
        df.loc[:, index_col] = range(1, len(df) + 1)
        return df
