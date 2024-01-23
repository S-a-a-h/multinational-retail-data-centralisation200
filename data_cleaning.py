from data_processing import DataProcessor


class DataCleaning(DataProcessor):
    '''
    DataCleaning Class
    -------
    This script cleans the data via the inherited class: DataProcessor.
    All methods and variables are named descriptively for readability.

    Methods
    -------
        * _clean_users_df - cleans the users data retrieved from an SQL database.
        * _clean_card_data - cleans the card details data retrieved from a pdf link.
        * _clean_b_store_data - cleans the business stores data retrieved from an S3 Bucket accessed via api-keys.
        * _convert_product_weights - converts and standardizes the weight values of the products data.
        * _clean_products_data - cleans the products data retrieved from an S3 Bucket accessed via the bucket address.
        * _clean_orders_df - cleans the orders data retrieved from the same SQL database as the users data.
        * _clean_sdt_df - cleans the sales date time data retrieved from an S3 Bucket accessed via the bucket url.
    '''

    #USERS_DF (users_df)
    def _clean_users_df(self, users_df):
        cleaned_users_df_c_code = DataProcessor.amend_users_country_code(users_df) 
        cleaned_users_df_add = DataProcessor.standardize_address(cleaned_users_df_c_code, 'address') 
        cleaned_users_df_jd = DataProcessor.standardize_date_column(cleaned_users_df_add, 'join_date')
        cleaned_users_df_dob = DataProcessor.standardize_date_column(cleaned_users_df_jd, 'date_of_birth')
        cleaned_users_df_uuid = DataProcessor.filter_uuids(cleaned_users_df_dob, ['user_uuid']) #PK
        cleaned_users_df_dup = cleaned_users_df_uuid.drop_duplicates() 
        clean_users_df = DataProcessor.fix_index(cleaned_users_df_dup, 'index')
        return clean_users_df
    

    #CARD_DF (card_df)
    def _clean_card_data(self, card_df): 
        card_df_cn = DataProcessor.filter_card_number(card_df) #PK
        cleaned_card_df_pdates = DataProcessor.standardize_date_column(card_df_cn, 'date_payment_confirmed') 
        cleaned_card_df_edates = DataProcessor.format_expiry_dates(cleaned_card_df_pdates) 
        cleaned_card_df_edates.drop_duplicates(inplace=True)
        cleaned_card_df_edates.dropna(subset=['date_payment_confirmed'], inplace=True)
        cleaned_card_df_edates.dropna()
        return cleaned_card_df_edates
    

    #BUSINESS STORE DF (b_store_df)
    def _clean_b_store_data(self, b_store_df): 
        cleaned_b_store_df_con = DataProcessor.amend_store_continent(b_store_df) 
        cleaned_b_store_df_num = DataProcessor.convert_to_numeric(cleaned_b_store_df_con, ['longitude', 'staff_numbers', 'latitude']) 
        cleaned_b_store_df_add = DataProcessor.standardize_address(cleaned_b_store_df_num, 'address') 
        cleaned_b_store_df_odate = DataProcessor.standardize_date_column(cleaned_b_store_df_add, 'opening_date') 
        cleaned_b_store_df_s_c = DataProcessor.filter_store_code(cleaned_b_store_df_odate) #PK
        cleaned_b_store_df_drop = DataProcessor.drop_df_column(cleaned_b_store_df_s_c, ['lat']) 
        cleaned_b_store_df_dup = cleaned_b_store_df_drop.drop_duplicates()         
        clean_b_store_df = DataProcessor.fix_index(cleaned_b_store_df_dup, 'index') 
        return clean_b_store_df
    

    #PRODUCTS DF (prods_df)
    def _convert_product_weights(self, prods_df):
        prods_df = DataProcessor.process_prod_weight(prods_df) 
        return prods_df
    
    def _clean_products_data(self, prods_df):
        cleaned_prods_df_col_n = DataProcessor.change_column_names(prods_df) 
        cleaned_prods_df_date = DataProcessor.standardize_date_column(cleaned_prods_df_col_n, 'date_added') 
        cleaned_prods_df_pc = DataProcessor.filter_product_code(cleaned_prods_df_date) #PK
        cleaned_prods_df_dup = cleaned_prods_df_pc.drop_duplicates() 
        cleaned_prods_df_dropna = cleaned_prods_df_dup.dropna(subset=['product_code'])
        clean_prods_df = DataProcessor.fix_index(cleaned_prods_df_dropna, 'index') 
        return clean_prods_df


    #ORDERS_DF (orders_df)
    def _clean_orders_df(self, orders_df):
        cleaned_orders_df_drop_cols = DataProcessor.drop_df_column(orders_df, ['level_0', 'first_name', 'last_name', '1']) 
        cleaned_orders_df_dup = cleaned_orders_df_drop_cols.drop_duplicates()
        clean_orders_df = DataProcessor.fix_index(cleaned_orders_df_dup, 'index')
        return clean_orders_df


    #SALES DATE TIMES (sdt_df)
    def _clean_sdt_df(self, sdt_df):
        cleaned_sdt_df_time = DataProcessor.standardize_timestamp(sdt_df) 
        cleaned_sdt_df_uuid = DataProcessor.filter_uuids(cleaned_sdt_df_time, ['date_uuid']) #PK
        clean_sdt_df = cleaned_sdt_df_uuid.drop_duplicates() 
        return clean_sdt_df
        

