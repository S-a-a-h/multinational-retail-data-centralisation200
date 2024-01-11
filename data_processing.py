import pandas as pd
import numpy as np
import re


class DataProcessor:
    #STORE_DF METHODS ONLY
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

    #USERS_DF METHODS ONLY
    @staticmethod
    def clean_users_email_address(users_df):
        users_df['email_address'] = users_df['email_address'].where(users_df['email_address'].astype(str).str.contains('@', na=False), None)
        return users_df
    
    @staticmethod
    def clean_users_country(users_df):
        valid_countries = ['Germany', 'United Kingdom', 'United States']
        users_df.loc[~users_df['country'].isin(valid_countries), 'country'] = np.nan
        return users_df
    
    @staticmethod
    def clean_users_country_code(users_df):
        valid_country_codes = ['GB', 'US', 'DE']
        country_code_mapping = {'GGB': 'GB'}

        def handle_GGB(c_code):
            return country_code_mapping.get(c_code, None) if c_code in valid_country_codes else None
        
        users_df['country_code'] = users_df['country_code'].apply(handle_GGB)
        return users_df

    @staticmethod
    def clean_users_company(users_df):
        pattern = re.compile(r'^[A-Za-z0-9]+$')
        users_df['company'] = users_df['company'].where(~users_df['company'].astype(str).str.match(pattern, na=False), None)
        return users_df
    

    #ORDERS_DF METHODS ONLY
    @staticmethod
    def clean_orders_store_code(order_df):
        pattern = r'^[A-Z0-9]{2}-[A-Z0-9]+$'
        order_df['store_code'] = order_df['store_code'].where(order_df['store_code'].str.match(pattern) | (order_df['store_code'] == 'WEB-1388012W'), None)
        return order_df


    #METHODS APPLICABLE TO MORE THAN 1 DF
    #method to convert dt to numeric then drop NaN values in those numeric columns
    @staticmethod
    def convert_and_drop_non_numeric(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce').where(df[column_name].notnull(), None)
        return df
    #(store_df, ['latitude', 'staff_numbers', 'longitude'])
    #(orders_df, ['product_quantity'])

    @staticmethod
    def clean_address(df, column_name):
        pattern = re.compile(r'^[A-Za-z0-9]+$')
        df[column_name] = df[column_name].apply(lambda x: None if pd.notna(x) and pattern.match(str(x)) else x.replace('\n', ''))
        return df
    #(store_df, 'address')
    #(users_df, 'address')  
    
    @staticmethod
    def clean_fnames_lnames(df):
        pattern = re.compile(r'^[A-Za-z0-9]+$')
        if 'first_name' in df.columns:
            df['first_name'] = df['first_name'].where(~df['first_name'].astype(str).str.contains(pattern, na=False) & ~df['first_name'].str.contains(r'\d'), None)
        if 'last_name' in df.columns:
            df['last_name'] = df['last_name'].where(~df['last_name'].astype(str).str.contains(pattern, na=False) & ~df['last_name'].str.contains(r'\d'), None)
        return df
    #(users_df)
    #(order_df)

    @staticmethod
    def clean_uuids(df, column_names):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = df[column_name].apply(lambda x: x if uuid_pattern.match(str(x)) else None)
        return df
    #(users_df, ['user_uuid'])
    #(orders_df, ['user_uuid', 'date_uuid'])
    
    @staticmethod
    def remove_invalid_dates(df, column_names):
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
                df[column_name] = df[column_name].where(df[column_name].notnull() & df[column_name].astype(str).str.match(date_pattern, na=False), None)
        return df
    #(store_df, ['opening_date'])
    #(users_df, ['join_date', 'date_of_birth'])

    @staticmethod
    def drop_df_cols(df, column_names):
        df.drop(columns=column_names)
        return df
    #(store_df, ['lat'])
    #(orders_df, ['level_0', '1'])

    @staticmethod
    def drop_null_values(df):
        df.dropna(how='all')
        return df
    #(store_df)
    #(users_df)
    #(orders_df)
    
    #method to drop duplicates in df
    @staticmethod
    def drop_duplicates(df):
        df = df.drop_duplicates()
        return df
    #(store_df)
    #(users_df)
    #(orders_df)

    #final method to correct index column
    @staticmethod
    def fix_index(df, index_col):
        df.set_index(index_col)
        df.index = df.index + 1 
        return df
    #(store_df, 'index')
    #(users_df, 'index')
    #(orders_df, 'index')
            
#all methods are static to avoid contantly having to create instances for each df in order to use the methods from this class
