import pandas as pd
import numpy as np

# class DataCleaning will clean data from each of the data sources
class DataCleaning:

    def __init__(self):
        pass
    
    # cleaned the user data
    def clean_user_data(self, users_table):
        # Drops the NULL from the table
        users_df = pd.read_csv('legacy_users.csv')
        users_df.replace('NULL', np.NaN, inplace=True)
        users_df.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

        # removes invalid dates from the Table
        users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
        users_df['join_date'] = pd.to_datetime(users_df['join_date'], infer_datetime_format=True, errors ='coerce')
        users_df = users_df.dropna(subset=['join_date'])

        # changes the GGB to GB for the country code
        users_df['country_code'] = users_df['country_code'].str.replace("GGB", "GB")

        # removing invalid characters from the phone number
        r = "[^0-9]+."
        users_df["phone_number"] = users_df["phone_number"].str.replace(r, "")
        # users_df["phone_number"] = users_df["phone_number"].str[-10:]

        # Adding country extensions to the front of phone numbers
        users_df["phone_number"] = users_df["country_code"].map({"DE": "0049 ", "GB": "0044 ", "US": "001 "}).astype(str) + users_df["phone_number"]

        # removes the duplicates of the email address
        users_df = users_df.drop_duplicates(subset=['email_address'])
        
        # removes the column 0 from the table
        users_df.drop(users_df.columns[0], axis=1, inplace=True)
        
        # action the above code to the csv file
        users_table.to_csv('legacy_users.csv')
        
        return users_df
        
cleaning = DataCleaning()
cleaning.clean_user_data(users_table=any)
