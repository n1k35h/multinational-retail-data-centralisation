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
        # if so, the NULL values replace with a 0 in the cell
        users_table.fillna(value=0, inplace=True)

        # Checks and remove invalid dates
        date_format = '%d/%m/%Y'
        users_table['date_of_birth'] = pd.to_datetime(users_table['date_of_birth'], format=date_format, errors = 'coerce')
        users_table['join_date'] = pd.to_datetime(users_table['join_date'], format=date_format, errors = 'coerce')
    
        # uploads to the csv file
        users_table.to_csv('legacy_users.csv')
        
cleaning = DataCleaning()
cleaning.clean_user_data(users_table=any)
