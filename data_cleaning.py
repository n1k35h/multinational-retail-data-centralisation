import pandas as pd
import numpy as np

# class DataCleaning will clean data from each of the data sources
class DataCleaning:

    def __init__(self):
        pass

    def clean_user_data(self, users_table):
    # clean the user data, look out for NULL values, error with dates, incorrect inputs and 
    # rows filled with incorrectly information
        
        # checks to see if there is a NULL values in the csv file
        users_table = pd.read_csv('legacy_users.csv')
        df = pd.DataFrame(users_table)
        # removes the NULL values with blank cells
        df.isnull().values.any()

        # Checks and remove invalid dates
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors = 'coerce')
    
        # uploads to the csv file
        users_table.to_csv('legacy_users.csv')
        
cleaning = DataCleaning()
cleaning.clean_user_data(users_table=any)
