import pandas as pd
import re


class DataProcessor:

    #store_df - processing and cleaning methods
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
    def clean_users_company(self, users_df):
        pattern = re.compile(r'[A-Z]+\d+')
        users_df.drop(users_df[users_df['company'].astype(str).str.contains(pattern, na=False)].index, inplace=True)
        return users_df

    def clean_users_email_address(self, users_df):
        users_df.drop(users_df[~users_df['email_address'].astype(str).str.contains('@', na=False)].index, inplace=True)
        return users_df
    
    def clean_users_country(self, users_df):
        users_df = users_df(users_df[~users_df['country'].isin(['Germany', 'United Kingdom', 'United States'])].index)
        return users_df
    
    def clean_users_country_code(self, users_df):
        filter_cc = users_df[~users_df['country_code'].isin(['GB', 'US', 'DE', 'GGB'])].index
        users_df.loc[filter_cc, 'country_code'] = users_df.loc[filter_cc, 'country_code'].apply(lambda x: x[1:] if x == 'GGB' else x)

    #orders_df - processing and cleaning methods
    def 




    #All dfs - remember to assign each df to the methods being used in turn inorder to process and update them  
    @staticmethod
    def clean_address(df, column_name):
        store_address = store_df['address'].str.split('\n', expand=True) #removes \n and splits the address into sections
        store_address.columns = ['street', 'city', 'postal_code', 'other_details']
        store_df = pd.concat([store_df, store_address], axis=1)
        store_df.drop(columns=['address'], inplace=True)
        return store_df
    #change the internal code of this method to static method!

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
    
    #WIP
    @staticmethod
    def remove_invalid_dates(df, column_names):


    def clean_store_odate(self, store_df):
        odate_to_remove = [ 'NULL', 'ZCXWWKF45G', '7AHXLXIUEF', '0OLAK2I6NS', 'A3PMVM800J', 'GMMB02LA9V', '13PIY8GD1H', '36IIMAQD58']
        store_df = store_df[~store_df['opening_date'].isin(odate_to_remove)]
        store_df['opening_dates'] = store_df['opening_dates'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notnull(pd.to_datetime(x, errors='coerce')) else x)
        return store_df
    
    users_df['join_date'].unique()
    >jdate_pattern = r'^\d{4}-\d{2}-\d{2}$'
    >non_standard_jdates = users_df[~users_df['join_date'].str.match(jdate_pattern)]
    >print(non_standard_jdates['join_date'].values) 
    >jdate_to_remove = ['NULL', 'JJ2PDVNPRO', 'AHN6EKASH3', 'FYF2FAPZF3', 'QH6Z9ZPX37', 'LYVWXBBI6F', 'DM4Q84QZ03', 'DOKMYDVV6L', 'YBUYH8T6OE', 'SRH5SM36LH', '4JIOCHZY0W', ‘3CUODA3HTC', '8BAER2328P', 'U9CRKSTONU', '9YLGYDEZNV', '7PF0SMLXII']
    >users_df = users_df[~users_df['join_date'].isin(jdate_to_remove)]
    >users_df['join_date'] = users_df['join_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notnull(pd.to_datetime(x, errors='coerce')) else x)

    users_df['date_of_birth'].unique()
    >dob_pattern = r'^\d{4}-\d{2}-\d{2}$'
    >non_standard_dob = users_df[~users_df['date_of_birth'].str.match(dob_pattern)]
    >print(non_standard_dob['date_of_birth'].values) 
    >dob_to_remove = ['NULL', 'KBTI7FI7Y3', 'OFH8YGZJWN', 'PQPEUO937L', '7KGJ3C5TSW', 'RQTF5XSXP4', 'D2OZZHWOLK', 'QTVEU5TR8H', 'L3E8OV4UAC', 'TLSTUEIKI0', 'YTC82GP4XE', 'O1LIA1MT1N', 'RQI3KQXFBQ', '1IA43NTJFB', 'M7HZECVQVC', '6WAQ2SVK0Q']
    >users_df = users_df[~users_df['date_of_birth'].isin(dob_to_remove)]
    >users_df['date_of_birth'] = users_df['date_of_birth'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notnull(pd.to_datetime(x, errors='coerce')) else x)
    #create static method to convert dates to datetime and drop all values which does not match the datetime format
    
    #WIP
    @staticmethod
    def remove_invalid_values(df, column_names):
    

    def clean_store_locality(self, store_df):
        locality_to_remove = ['N/A',  'NULL', '9IBH8Y4Z0S', '1T6B406CI8', '6LVWPU1G64', 'RX9TCP2RGB', 'CQMHKI78BX', 'RY6K0AUE7F', '3VHFDNP8ET']
        store_df = store_df[~store_df['locality'].isin(locality_to_remove)]
        return store_df
    
    def clean_store_code(self, store_df):
        codes_to_remove = ['NULL', 'NRQKZWJ9OZ', 'QIUU9SVP51', 'Y8J0Z2W8O9', 'ISEE8A57FE', 'T0R2CQBDUS', 'TUOKF5HAAQ', '9D4LK7X4LZ']
        store_df = store_df[~store_df['store_code'].isin(codes_to_remove)]
        return store_df
    #create static method to drop all values which does not match the invlaid format (make a pattern) or is a NULL or N/A value
    
    #WIP
    @staticmethod
    def clean_fnames_lnames(df, column_names):
        pattern = re.compile(r'\d') 
        numeric_first_names = users_df[users_df['first_name'].astype(str).str.contains(pattern, na=False)].index
        numeric_last_names = users_df[users_df['last_name'].astype(str).str.contains(pattern, na=False)].index
    #edit this method to make it staticmethod