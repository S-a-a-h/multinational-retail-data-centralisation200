import pandas as pd
import re


class DataTransformer:

    #store_df - transforming and cleaning methods
    def drop_store_cols(self, store_df):
        store_df = store_df.drop(['index', 'lat'], axis=1, inplace=True)
        return store_df

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
        store_df['locality'].unique()
        locality_to_remove = ['N/A',  '9IBH8Y4Z0S', '1T6B406CI8', 'NULL', '6LVWPU1G64', 'RX9TCP2RGB', 'CQMHKI78BX', 'RY6K0AUE7F', '3VHFDNP8ET']
        store_df = store_df[~store_df['locality'].isin(locality_to_remove)]
        return store_df
    
    def clean_store_code(self, store_df):
        codes_to_remove = ['WEB-1388012W', 'NRQKZWJ9OZ', 'QIUU9SVP51', 'NULL', 'Y8J0Z2W8O9', 'ISEE8A57FE', 'T0R2CQBDUS', 'NULL', 'TUOKF5HAAQ', 'NULL', '9D4LK7X4LZ']
        store_df = store_df[~store_df['store_code'].isin(codes_to_remove)]
        return store_df

    def clean_store_type(self, store_df):
        type_to_remove = ['QP74AHEQT0', 'O0QJIRC943', 'NULL', '50IB01SFAZ', '0RSNUU3DF5', 'B4KVQB3P5Y', 'X0FE7E2EOG', 'NN04B3F6UQ']
        store_df = store_df[~store_df['store_type'].isin(type_to_remove)]
        return store_df

    def clean_store_country_code(self, store_df):
        store_df = store_df(store_df[~store_df['country_code'].isin(['GB', 'US', 'DE'])].index)
        return store_df

    def clean_store_continent(self, store_df):
        continent_to_remove = ['QMAVR5H3LD', 'LU3E036ZD9', 'NULL', '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        store_df = store_df[~store_df['continent'].isin(continent_to_remove)]
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