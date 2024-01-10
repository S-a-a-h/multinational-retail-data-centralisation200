import pandas as pd
import re


class DataProcessor:

    #store_df - processing and cleaning methods
    def clean_store_odate(self, store_df):
        odate_to_remove = ['ZCXWWKF45G', '7AHXLXIUEF', 'NULL', '0OLAK2I6NS', 'A3PMVM800J', 'GMMB02LA9V', 'NULL', '13PIY8GD1H', 'NULL', '36IIMAQD58']
        store_df = store_df[~store_df['opening_date'].isin(odate_to_remove)]
        store_df['opening_dates'] = store_df['opening_dates'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notnull(pd.to_datetime(x, errors='coerce')) else x)
        return store_df

    def clean_store_address(self, store_df):
        store_address = store_df['address'].str.split('\n', expand=True) #removes \n and splits the address into sections
        store_address.columns = ['street', 'city', 'postal_code', 'other_details']
        store_df = pd.concat([store_df, store_address], axis=1)
        store_df.drop(columns=['address'], inplace=True)
        return store_df

    def clean_store_locality(self, store_df):
        locality_to_remove = ['N/A',  '9IBH8Y4Z0S', '1T6B406CI8', 'NULL', '6LVWPU1G64', 'RX9TCP2RGB', 'CQMHKI78BX', 'RY6K0AUE7F', '3VHFDNP8ET']
        store_df = store_df[~store_df['locality'].isin(locality_to_remove)]
        return store_df
    
    def clean_store_code(self, store_df):
        codes_to_remove = ['NRQKZWJ9OZ', 'QIUU9SVP51', 'NULL', 'Y8J0Z2W8O9', 'ISEE8A57FE', 'T0R2CQBDUS', 'NULL', 'TUOKF5HAAQ', 'NULL', '9D4LK7X4LZ']
        store_df = store_df[~store_df['store_code'].isin(codes_to_remove)]
        return store_df

    def clean_store_type(self, store_df):
        store_df = store_df(store_df[~store_df['store_type'].isin(['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal'])].index)
        return store_df

    def clean_store_country_code(self, store_df):
        store_df = store_df(store_df[~store_df['country_code'].isin(['GB', 'US', 'DE'])].index)
        return store_df

    def clean_store_continent(self, store_df):
        store_df = store_df(store_df[~store_df['continent'].isin(['Europe', 'America', 'eeEurope', 'eeAmerica'])].index)
        store_df['continent'] = store_df['continent'].apply(lambda x: x[2:] if x.startswith('ee') else x)
        return store_df


    #users_df - processing and cleaning methods
    def 





    #orders_df - processing and cleaning methods
    def 




    #All dfs - remember to assign each df to the methods being used in turn inorder to process and update them  
    @staticmethod
    def drop_rows_with_numeric(df, columns): 
        pattern = re.compile(r'\d')
        for col in columns:
            df = df[~df[col].astype(str).str.contains(pattern, na=False)]
        return df
    #use this method to filter:(users_df, 'first_name', 'last_name')
    #use this method to filter:(orders_df, 'first_name', 'last_name')

    @staticmethod
    def clean_uuids(df, column_name):
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        df[column_name] = df[column_name].apply(lambda x: x if uuid_pattern.match(str(x)) else None)
        return df
    #(users_df, 'user_uuid')
    #(orders_df, 'user_uuid', 'date_uuid')

    #numeric columns method to convert dt then drop NaN values in the columns
    @staticmethod
    def convert_and_drop(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            df.dropna(subset=[column_name], inplace=True) #drops NaN values
        return df
        
    @staticmethod
    def drop_duplicates(df):
        df = df.drop_duplicates()
        return df
    
    @staticmethod
    def drop_df_cols(df, column_names):
        df = df.drop(columns=column_names, inplace=True)
    #(store_df, 'lat')
    #(orders_df, 'level_0', '1')
    
    @staticmethod
    def fix_index(df, index_col):
        df.set_index(index_col, inplace=True)
    

