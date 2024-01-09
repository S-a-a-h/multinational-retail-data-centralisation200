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
        codes_to_remove = ['WEB-1388012W', 'NRQKZWJ9OZ', 'QIUU9SVP51', 'NULL', 'Y8J0Z2W8O9', 'ISEE8A57FE', 'T0R2CQBDUS', 'NULL', 'TUOKF5HAAQ', 'NULL', '9D4LK7X4LZ']
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
    
    #numeric columns method to convert dt then drop NaN values in the columns
    def convert_and_drop(self, df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            df.dropna(subset=[column_name], inplace=True) #drops NaN values
            return df


    #users_df - transforming and cleaning methods
    def 





    #orders_df - transforming and cleaning methods
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

    #numeric columns method to convert dt then drop NaN values in the columns
    @staticmethod
    def convert_and_drop(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            df.dropna(subset=[column_name], inplace=True) #drops NaN values
            return df
        
    @staticmethod
    def drop_duplicates(df):
        cleaned_df = df.drop_duplicates()
        return cleaned_df
    
    @staticmethod
    def drop_df_cols(df, column_names):
        clean_df = df.drop(columns=column_names, inplace=True)
        return clean_df 
    #(store_df, 'lat')
    #(orders_df, '1')
    
    @staticmethod
    def fix_index(df, index_col):
        df.set_index(index_col, inplace=True)
