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
    
    @staticmethod # - CLEANED (not over-engineered)
    def clean_users_country_code(users_df):
        valid_country_codes = ['GB', 'US', 'DE']
        country_code_mapping = {'GGB': 'GB'}
        users_df.loc[:, 'country_code'] = users_df['country_code'].apply(lambda c_c: country_code_mapping.get(c_c, c_c))
        users_df = users_df[users_df['country_code'].isin(valid_country_codes)]
        return users_df
    

    #CARD_DF METHODS ONLY
    @staticmethod # - CLEANED (not over-engineered)
    def card_prov(card_df):
        valid_card_provs = ['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit', 'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover', 'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']
        card_df = card_df[card_df['card_provider'].isin(valid_card_provs)]
        return card_df

    @staticmethod # - CLEANED (not over-engineered)
    def clean_store_edate(card_df, column_name):
        date_pattern = re.compile(r'^(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
        card_df = card_df.copy()
        if column_name in card_df.columns:
            card_df[column_name] = card_df[column_name].astype(str).replace('/', '-', regex=True)
            card_df = card_df.loc[card_df[column_name].apply(lambda x: bool(date_pattern.match(x)) if x is not None else False)]
        return card_df
    #(card_df, 'expiry_date')

    @staticmethod # - CLEANED (not over-engineered)
    def clean_card_number(card_df, column_name):
        card_df = card_df.copy()
        card_df[column_name] = card_df[column_name].apply(lambda x: x if str(x).isdigit() else x)
        return card_df
    #(card_df, 'card_number')


    #STORE_DF METHODS ONLY - fix these methods as they are currently over-engineered!
    @staticmethod
    def clean_store_type(store_df):
        valid_store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        store_df['store_type'] = store_df['store_type'].where(store_df['store_type'].isin(valid_store_types), None)
        return store_df
    
    @staticmethod
    def clean_store_country_code(store_df):
        valid_country_codes = ['GB', 'US', 'DE']
        store_df['country_code'] = store_df['country_code'].where(store_df['country_code'].isin(valid_country_codes), None)
        return store_df

    @staticmethod
    def clean_store_continent(store_df):
        continents_to_keep = ['Europe', 'America', 'eeEurope', 'eeAmerica']
        store_df['continent'] = store_df['continent'].where(store_df['continent'].isin(continents_to_keep), None)
        store_df['continent'] = store_df['continent'].apply(lambda x: x[2:] if x and x.startswith('ee') else x)
        return store_df
    
    @staticmethod
    def remove_invalid_values(df, column_names):
        pattern = re.compile(r'^[A-Za-z0-9]{10}$')
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].where(df[column_name].astype(str).str.match(pattern, na=False), None)
        return df
    #(store_df, ['locality', 'store_code'])

    #ORDERS_DF METHODS ONLY - fix these methods as they are currently over-engineered!
    @staticmethod
    def clean_orders_store_code(order_df):
        pattern = r'^[A-Z0-9]{2}-[A-Z0-9]+$'
        order_df['store_code'] = order_df['store_code'].where(order_df['store_code'].str.match(pattern) | (order_df['store_code'] == 'WEB-1388012W'), None)
        return order_df


    #METHODS APPLICABLE TO MORE THAN 1 DF     
    @staticmethod # - CLEANED (not over-engineered)
    def tonumeric_and_drop_non_numeric(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        df.dropna(subset=column_names, how='any', inplace=True)
        return df
    #(store_df, ['latitude', 'staff_numbers', 'longitude'])
    #(orders_df, ['product_quantity'])

    @staticmethod # - CLEANED (not over-engineered)
    def clean_address(df, column_name):
        df[column_name] = df[column_name].apply(lambda x: str(x).replace('\n', ' ') if pd.notna(x) else x)
        return df
    #(store_df, 'address')
    #(users_df, 'address')  

    @staticmethod # - CLEANED (not over-engineered)
    def clean_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].apply(lambda x: x if uuid_pattern.match(str(x)) else x)
        return df
    #(users_df, ['user_uuid'])
    #(orders_df, ['user_uuid', 'date_uuid'])

    @staticmethod # - CLEANED (not over-engineered)
    def clean_dates(df, column_names):
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        for column_name in column_names:
            if column_name in df.columns:
                converted_dates = pd.to_datetime(df[column_name], errors='coerce', format='%Y-%m-%d')
                df.loc[:, column_name] = converted_dates
                non_conforming_mask = ~converted_dates.notnull() | ~converted_dates.astype(str).str.match(date_pattern, na=False)
                df.loc[non_conforming_mask, column_name] = pd.NaT
        df = df.dropna(subset=column_names)
        return df
    #(store_df, ['opening_date'])
    #(users_df, ['join_date', 'date_of_birth'])

    @staticmethod # - CLEANED (not over-engineered)
    def drop_df_cols(df, column_names):
        df.drop(columns=column_names, inplace=True)
        return df
    #(store_df, ['lat'])
    #(orders_df, ['level_0', '1'])
    
    #method to drop duplicates in df
    @staticmethod # - CLEANED (not over-engineered)
    def drop_duplicates(df):
        df = df.copy()
        df = df.drop_duplicates()
        return df
    #(store_df)
    #(users_df)
    #(orders_df)

    #final method to correct index column - CLEANED (not over-engineered)
    @staticmethod # - CLEANED (not over-engineered)
    def fix_index(df, index_col):
        df.reset_index(drop=True, inplace=True)
        df.loc[:, index_col] = range(1, len(df) + 1)
        return df
    #(store_df, 'index')
    #(users_df, 'index')
    #(orders_df, 'index')
            
#all methods are static to avoid contantly having to create instances for each df in order to use the methods from this class
