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
    def clean_users_email_address(self, users_df):
        users_df.drop(users_df[~users_df['email_address'].astype(str).str.contains('@', na=False)].index, inplace=True)
        return users_df
    
    def clean_users_country(self, users_df):
        users_df = users_df(users_df[~users_df['country'].isin(['Germany', 'United Kingdom', 'United States'])].index)
        return users_df
    
    def clean_users_country_code(self, users_df):
        filter_cc = users_df[~users_df['country_code'].isin(['GB', 'US', 'DE', 'GGB'])].index
        users_df.loc[filter_cc, 'country_code'] = users_df.loc[filter_cc, 'country_code'].apply(lambda x: x[1:] if x == 'GGB' else x)

    def clean_users_company(self, users_df):
        pattern = re.compile(r'[A-Z]+\d+')
        users_df.drop(users_df[users_df['company'].astype(str).str.contains(pattern, na=False)].index, inplace=True)
        return users_df
    
    #orders_df - processing and cleaning methods
    def 




    #All dfs - remember to assign each df to the methods being used in turn inorder to process and update them  
        
    #method to drop duplicates in a df
    @staticmethod
    def drop_duplicates(df):
        df = df.drop_duplicates()
        return df
    
    @staticmethod
    def remove_invalid_values(df, column_names):
        pattern = re.compile(r'^[A-Za-z0-9]{10}$')
        for column_name in column_names:
            if column_name in df.columns:
                df = df[~(df[column_name].astype(str).str.match(pattern, na=False) | df[column_name].isnull())]
        return df
    #(store_df, ['locality', 'store_code'])

    #method to convert dt to numeric then drop NaN values in those numeric columns
    @staticmethod
    def convert_and_drop_non_numeric(df, column_names):
        for column_name in column_names:
            df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
            df.dropna(subset=[column_name], inplace=True) #drops NaN values
        return df
    #(store_df, ['latitude', 'staff_numbers', 'longitude'])

    @staticmethod
    def clean_address(df, column_name):
        df[column_name] = df[column_name].str.replace('\n', '', regex=True)
        df.dropna(subset=[column_name], inplace=True)
        pattern = re.compile(r'^[A-Za-z0-9]{10}$')
        df = df[~df[column_name].str.match(pattern, na=False)]
        return df
    #(store_df, 'address')
    #(users_df, 'address')  

    @staticmethod
    def drop_rows_with_numeric(df, column_names): 
        pattern = re.compile(r'\d')
        for col in column_names:
            df = df[~df[col].astype(str).str.contains(pattern, na=False)]
        return df
    #(users_df, ['first_name', 'last_name'])
    #(orders_df, ['first_name', 'last_name'])

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
    def drop_df_cols(df, column_names):
        df = df.drop(columns=column_names, inplace=True)
    #(store_df, ['lat'])
    #(orders_df, ['level_0', '1'])
    
    @staticmethod
    def remove_invalid_dates(df, column_names):
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        for column_name in column_names:
            if column_name in df.columns:
                df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
                df = df[df[column_name].notnull() & df[column_name].astype(str).str.match(date_pattern, na=False)]
        return df
    #(store_df, ['opening_date'])
    #(users_df, ['join_date', 'date_of_birth'])

    @staticmethod
    def clean_fnames_lnames(df):
        pattern = re.compile(r'\d') 
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df = df.loc[~(df['first_name'].astype(str).str.contains(pattern, na=False) | df['first_name'].isnull())]
            df = df.loc[~(df['last_name'].astype(str).str.contains(pattern, na=False) | df['last_name'].isnull())]
        return df
    #(users_df)
    #(order_df)




    #final method to correct index column
    @staticmethod
    def fix_index(df, index_col):
        df.set_index(index_col, inplace=True)
        df.index = df.index + 1 
    #(store_df, 'index')
    #(users_df, 'index')
    #(orders_df, 'index')