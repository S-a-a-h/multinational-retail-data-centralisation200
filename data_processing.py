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
    def clean_store_edate(card_df):
        date_pattern = re.compile(r'^(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
        card_df = card_df.copy()
        if 'expiry_date' in card_df.columns:
            card_df['expiry_date'] = card_df['expiry_date'].astype(str).replace('/', '-', regex=True)
            card_df = card_df.loc[card_df['expiry_date'].apply(lambda edate: bool(date_pattern.match(edate)) if edate is not None else False)]
        return card_df


    #B_STORE_DF METHODS ONLY 
    @staticmethod
    def clean_store_continent(b_store_df):
        b_store_df['continent'] = b_store_df['continent'].apply(lambda con: con[2:] if con and con.startswith('ee') else con)
        return b_store_df
    
    @staticmethod
    def tonumeric_and_drop_non_numeric(b_store_df, column_names):
        for column_name in column_names:
            b_store_df[column_name] = pd.to_numeric(b_store_df[column_name], errors='coerce')
        b_store_df.dropna(subset=column_names, how='any', inplace=True)
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


    #SDT_DF METHODS ONLY
    @staticmethod #does not drop rows!!!!!!!
    def clean_timestamp(sdt_df):
        sdt_df['timestamp'] = pd.to_datetime(sdt_df['timestamp'], errors='coerce')
        sdt_df['timestamp'] = sdt_df['timestamp'].dt.strftime('%H:%M:%S')
        return sdt_df


    #METHODS APPLICABLE TO MORE THAN 1 DF 
    @staticmethod
    def clean_card_number(df, column_name):
        df[column_name] = df[column_name].apply(lambda card_num: card_num if str(card_num).isdigit() else card_num)
        return df

    @staticmethod
    def clean_address(df, column_name):
        df[column_name] = df[column_name].apply(lambda address: str(address).replace('\n', ' ') if pd.notna(address) else address)
        return df  

    @staticmethod 
    def clean_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].apply(lambda uuid: uuid if uuid_pattern.match(str(uuid)) else uuid)

        df = df[df[column_names].apply(lambda uuid: all(uuid_pattern.match(str(value)) for value in uuid), axis=1)]
        return df

    @staticmethod
    def clean_dates(df, column_names):
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        for column_name in column_names:
            if column_name in df.columns:
                converted_dates = pd.to_datetime(df[column_name], errors='coerce', format='%Y-%m-%d')
                df.loc[:, column_name] = converted_dates.dt.strftime('%Y-%m-%d')
                non_conforming_mask = ~converted_dates.notnull() | ~converted_dates.astype(str).str.match(date_pattern, na=False)
                df.loc[non_conforming_mask, column_name] = pd.NaT
        df = df.dropna(subset=column_names)
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
