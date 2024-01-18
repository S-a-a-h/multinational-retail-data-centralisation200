import pandas as pd
import re


class DataProcessor:
    '''
    DataProcessor Class
    -------
    This script transforms the data by processing, correcting values and dropping any invalid rows from each DataFrame.
    The methods included are all staticmethods for code reusablity on all DataFrames created in this project.
    All methods and variables are named descriptively for readability.
    '''

    #USERS_DF METHODS ONLY 
    @staticmethod
    def clean_users_country_code(users_df):
        valid_country_codes = ['GB', 'US', 'DE']
        country_code_mapping = {'GGB': 'GB'}
        users_df.loc[:, 'country_code'] = users_df['country_code'].apply(lambda c_c: country_code_mapping.get(c_c, c_c))
        users_df = users_df[users_df['country_code'].isin(valid_country_codes)]
        return users_df


    #CARD_DF METHODS ONLY
    @staticmethod
    def format_expiry_dates(card_df):
        if 'expiry_date' in card_df.columns:
            try:
                card_df['expiry_date'] = pd.to_datetime(card_df['expiry_date'], format='%m-%d')
            except ValueError:
                card_df['expiry_date'] = card_df['expiry_date']  # Keep the original values in case of an error
        return card_df


    #B_STORE_DF METHODS ONLY 
    @staticmethod
    def clean_store_code(b_store_df):
        pattern = r'^[A-Z]{2}-\w{8}$|^WEB-1388012W$'
        mask = b_store_df['store_code'].str.match(pattern, na=False)
        b_store_df = b_store_df[mask]
        return b_store_df

    @staticmethod
    def clean_store_continent(b_store_df):
        b_store_df['continent'] = b_store_df['continent'].apply(lambda con: con[2:] if con and con.startswith('ee') else con)
        return b_store_df
    
    @staticmethod
    def tonumeric(b_store_df, column_names):
        for column_name in column_names:
            b_store_df[column_name] = pd.to_numeric(b_store_df[column_name], errors='coerce')
        return b_store_df


    #PRODS_DF METHODS ONLY
    @staticmethod
    def convert_weight(weight):
        if pd.isna(weight):
            return weight

        if 'g' in weight or 'ml' in weight:
            numeric_value = pd.to_numeric(''.join(char for char in weight if char.isdigit() or char == '.'), errors='coerce') / 1000
            return numeric_value if not pd.isna(numeric_value) else weight
        else:
            return pd.to_numeric(weight, errors='coerce')

    @staticmethod
    def process_prod_weight(prods_df):
        prods_df['weight'] = prods_df['weight'].str.replace('kg', '')
        prods_df['weight'] = prods_df['weight'].apply(DataProcessor.convert_weight)
        return prods_df
    
    @staticmethod
    def clean_col_names(prods_df):
        prods_df = prods_df.rename(columns={'Unnamed: 0': 'index'})
        prods_df = prods_df.rename(columns={'weight': 'weight(kg)'})
        prods_df = prods_df.rename(columns={'removed': 'product_status'})
        return prods_df
    
    @staticmethod
    def clean_prod_codes(prods_df):
        pattern = r'^[A-Z0-9]{2}-\d{7}[a-z]$'
        mask = prods_df['product_code'].str.match(pattern, na=False)
        prods_df = prods_df[mask]
        return prods_df
    

    #SDT_DF METHODS ONLY
    @staticmethod #does not drop rows!!!!!!!
    def clean_timestamp(sdt_df):
        sdt_df['timestamp'] = pd.to_datetime(sdt_df['timestamp'], errors='coerce')
        sdt_df['timestamp'] = sdt_df['timestamp'].dt.strftime('%H:%M:%S')
        return sdt_df


    #METHODS APPLICABLE TO MORE THAN 1 DF 
    @staticmethod
    def clean_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        mask = df[column_names].apply(lambda col: col.astype(str).str.match(uuid_pattern, na=False)).all(axis=1)
        df = df[mask]
        return df

    
    @staticmethod
    def clean_card_number(df, column_name):
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        df.dropna(subset=[column_name], inplace=True)
        return df
    
    @staticmethod
    def clean_card_number(df, column_name):
        df[column_name] = df[column_name].apply(lambda card_num: card_num if str(card_num).isdigit() else card_num)
        return df

    @staticmethod
    def clean_address(df, column_name):
        df[column_name] = df[column_name].apply(lambda address: str(address).replace('\n', ' ') if pd.notna(address) else address)
        return df  

    @staticmethod
    def clean_dates(df, column_names):
        df[column_names] = df[column_names].apply(pd.to_datetime, errors='coerce')
        return df

    @staticmethod
    def drop_df_cols(df, column_names):
        df.drop(columns=column_names, inplace=True)
        return df
    
    @staticmethod 
    def drop_duplicates(df):
        df = df.copy()
        df = df.drop_duplicates()
        return df

    @staticmethod 
    def fix_index(df, index_col):
        df.reset_index(drop=True, inplace=True)
        df.loc[:, index_col] = range(1, len(df) + 1)
        return df
