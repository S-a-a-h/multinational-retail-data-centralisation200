import pandas as pd
import numpy as np
import re


class DataProcessor:
    #USERS_DF METHODS ONLY 
    @staticmethod # - CLEANED (not over-engineered)
    def clean_users_country(users_df):
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        users_df = users_df[users_df['country'].isin(valid_countries)]
        return users_df
    #(users_df)
    
    @staticmethod # - CLEANED (not over-engineered)
    def clean_users_country_code(users_df):
        valid_country_codes = ['GB', 'US', 'DE']
        country_code_mapping = {'GGB': 'GB'}
        users_df.loc[:, 'country_code'] = users_df['country_code'].apply(lambda c_c: country_code_mapping.get(c_c, c_c))
        users_df = users_df[users_df['country_code'].isin(valid_country_codes)]
        return users_df
    #(users_df)
    

    #CARD_DF METHODS ONLY
    @staticmethod # - CLEANED (not over-engineered)
    def card_prov(card_df):
        valid_card_provs = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        card_df = card_df[card_df['card_provider'].isin(valid_card_provs)]
        return card_df
    #(card_df)

    @staticmethod # - CLEANED (not over-engineered)
    def clean_store_edate(card_df):
        date_pattern = re.compile(r'^(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
        card_df = card_df.copy()
        if 'expiry_date' in card_df.columns:
            card_df['expiry_date'] = card_df['expiry_date'].astype(str).replace('/', '-', regex=True)
            card_df = card_df.loc[card_df['expiry_date'].apply(lambda x: bool(date_pattern.match(x)) if x is not None else False)]
        return card_df
    #(card_df)


    #B_STORE_DF METHODS ONLY 
    @staticmethod # - CLEANED (not over-engineered)
    def clean_store_continent(b_store_df):
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        b_store_df['continent'] = b_store_df['continent'].where(b_store_df['continent'].isin(continents_to_keep), None)
        b_store_df['continent'] = b_store_df['continent'].apply(lambda x: x[2:] if x and x.startswith('ee') else x)
        return b_store_df
    #(b_store_df)  


    #PRODS_DF METHODS ONLY
    @staticmethod # - CLEANED (not over-engineered)
    def process_prod_weight(prods_df):
        prods_df['weight'] = prods_df['weight'].astype(str).str.replace('kg', '')
        
        def convert_weight(weight):
            if 'g' in weight or 'ml' in weight:
                numeric_value = pd.to_numeric(''.join(char for char in weight if char.isdigit() or char == '.'), errors='coerce') / 1000
                return numeric_value if not pd.isna(numeric_value) else weight
            else:
                return pd.to_numeric(weight, errors='coerce')

        prods_df['weight'] = prods_df['weight'].apply(convert_weight)
        return prods_df
    
    @staticmethod # - CLEANED (not over-engineered)
    def clean_col_names(prods_df):
        prods_df = prods_df.rename(columns={'Unnamed: 0': 'index'})
        prods_df = prods_df.rename(columns={'weight': 'weight(kg)'})
        prods_df = prods_df.rename(columns={'removed': 'product_status'})
        return prods_df
    
    @staticmethod # - CLEANED (not over-engineered)
    def clean_EAN(prods_df):
        pattern = r'^\d+$'
        prods_df = prods_df[prods_df['EAN'].astype(str).str.match(pattern)]
        return prods_df


    #SDT_DF METHODS ONLY
    @staticmethod
    def clean_timestamp(sdt_df):
        sdt_df['timestamp'] = pd.to_datetime(sdt_df['timestamp'], errors='coerce')
        sdt_df['timestamp'] = sdt_df['timestamp'].dt.strftime('%H:%M:%S')
        return sdt_df

    @staticmethod
    def clean_time_period(sdt_df):
        valid_periods = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        sdt_df = sdt_df[sdt_df['time_period'].isin(valid_periods)]
        return sdt_df


    #METHODS APPLICABLE TO MORE THAN 1 DF 
    @staticmethod
    def clean_card_number(df, column_name):
        df[column_name] = df[column_name].apply(lambda x: x if str(x).isdigit() else x)
        return df
    #(card_df, 'card_number')
    #(orders_df, 'card_number')
        
    @staticmethod # - CLEANED (not over-engineered)
    def tonumeric_and_drop_non_numeric(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        df.dropna(subset=column_names, how='any', inplace=True)
        return df
    #(b_store_df, ['longitude', 'staff_numbers', 'latitude'])
    #(orders_df 'product_quantity')
    #(sdt_df, ['month', 'year', 'day'])

    @staticmethod # - CLEANED (not over-engineered)
    def clean_address(df, column_name):
        df[column_name] = df[column_name].apply(lambda x: str(x).replace('\n', ' ') if pd.notna(x) else x)
        return df
    #(users_df, 'address')
    #(b_store_df, 'address')    

    @staticmethod # - CLEANED (not over-engineered)
    def clean_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].apply(lambda x: x if uuid_pattern.match(str(x)) else x)
        return df
    #(users_df, ['user_uuid'])
    #(prods_df, ['uuid'])
    #(orders_df, ['date_uuid', 'user_uuid'])
    #(sdt_df, ['date_uuid'])

    @staticmethod # - CLEANED (not over-engineered)
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
    #(users_df, ['join_date', 'date_of_birth'])
    #(card_df, ['date_payment_confirmed'])
    #(b_store_df, ['opening_date'])
    #(prods_df, ['date_added'])

    @staticmethod # - CLEANED (not over-engineered)
    def drop_df_cols(df, column_names):
        df.drop(columns=column_names, inplace=True)
        return df
    #(card_df, ['card_number expiry_date', 'Unnamed: 0'])
    #(b_store_df, ['lat'])
    #(orders_df, ['level_0', 'first_name', 'last_name', '1'])
    
    #method to drop duplicates in df
    @staticmethod # - CLEANED (not over-engineered)
    def drop_duplicates(df):
        df = df.copy()
        df = df.drop_duplicates()
        return df
    #(users_df)
    #(card_df)
    #(b_store_df)
    #(prods_df)
    #(orders_df)
    #(sdt_df)

    @staticmethod # - CLEANED (not over-engineered)
    def fix_index(df, index_col):
        df.reset_index(drop=True, inplace=True)
        df.loc[:, index_col] = range(1, len(df) + 1)
        return df
    #(users_df, 'index')
    #(b_store_df, 'index')
    #(prods_df, 'index')
    #(orders_df, 'index')
            
#all methods are static to avoid contantly having to create instances for each df in order to use the methods from this class
