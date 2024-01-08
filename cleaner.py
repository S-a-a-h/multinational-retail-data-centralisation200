import pandas as pd


class DataCleaner:

    def __init__(self):
        pass

    @staticmethod
    def clean_store_data(legacy_store_details):
        #legacy_store_details['country_code'].unique()
        legacy_store_details = legacy_store_details.drop(legacy_store_details[~legacy_store_details['country_code'].isin(['GB', 'US', 'DE'])].index)
        return legacy_store_details


    def hello(self, word):
        #legacy_store_details['country_code'].unique()
        #store_df = store_df.drop(store_df[~store_df['country_code'].isin(['GB', 'US', 'DE'])].index)
        #return store_df
        print(word)