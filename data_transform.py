#Create all cleaning processes in this class 
#As a lot of what's wrong in one table might be the same thing in another table
#Also don't worry about the types too much here as milestone 3 asks you to change the types

#Other tips as well categorical columns are great to target with df["column_name"].unique()
#As they don't have many values so if there are errors with their rows they'll show up in the unique list of values
#Also in a later milestone you're going to be connecting some tables together with a relationship
#If that relationship doesn't work then you know there's something wrong with the cleaning
import pandas as pd
import re


class DataTransformer:

    #store_df - transforming and cleaning methods
    def clean_store_address():
        store_address = store_df['address'].str.split('\n', expand=True) #removes \n and splits the address into sections
        store_address.columns = ['street', 'city', 'postal_code', 'other_details']
        store_df = pd.concat([store_df, store_address], axis=1)
        store_df.drop(columns=['address'], inplace=True)

    def clean_store_locality():
        store_df['locality'].unique()
        locality_to_remove = ['N/A',  '9IBH8Y4Z0S', '1T6B406CI8', 'NULL', '6LVWPU1G64', 'RX9TCP2RGB', 'CQMHKI78BX', 'RY6K0AUE7F', '3VHFDNP8ET']
        store_df = store_df[~store_df['locality'].isin(locality_to_remove)]
    
    def clean_store_code():
        codes_to_remove = ['WEB-1388012W', 'NRQKZWJ9OZ', 'QIUU9SVP51', 'NULL', 'Y8J0Z2W8O9', 'ISEE8A57FE', 'T0R2CQBDUS', 'NULL', 'TUOKF5HAAQ', 'NULL', '9D4LK7X4LZ']
        store_df = store_df[~store_df['store_code'].isin(codes_to_remove)]

    def clean_store_type():
        type_to_remove = ['QP74AHEQT0', 'O0QJIRC943', 'NULL', '50IB01SFAZ', '0RSNUU3DF5', 'B4KVQB3P5Y', 'X0FE7E2EOG', 'NN04B3F6UQ']
        store_df = store_df[~store_df['store_type'].isin(type_to_remove)]

    def clean_store_country_code():
        store_df = store_df(store_df[~store_df['country_code'].isin(['GB', 'US', 'DE'])].index)

    def clean_store_continent():
        continent_to_remove = ['QMAVR5H3LD', 'LU3E036ZD9', 'NULL', '5586JCLARW', 'GFJQ2AAEQ8', 'SLQBD982C0', 'XQ953VS0FG', '1WZB1TE1HL']
        store_df = store_df[~store_df['continent'].isin(continent_to_remove)]
        store_df['continent'] = store_df['continent'].apply(lambda x: x[2:] if x.startswith('ee') else x)


    #users_df - transforming and cleaning methods
    def 





    #orders_df - transforming and cleaning methods
    def 