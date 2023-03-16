from database_utils import DatabaseConnector
import pandas as pd

# DataExtractor will act as a Utility class, create methods that help extract data from different
# data sources such as; CSV files, an API and an S3 bucket.
class DataExtractor:
    def __init__(self):
        pass

    def read_rds_table(self, table_name):
    # this method will extract the database table to a pandas DataFrame
    # taking an instance of the DatabaseConnector class and the table name as an argument and return pandas DataFrame
    # list_db_tables method take name of the table containing user data
    # read_rds_table method extract the table containing user data and return a pandas DataFrame
        database_connector = DatabaseConnector() 
        engine = database_connector.init_db_engine()
        user_data = pd.read_sql_table(table_name, engine)
        return user_data
    

data_extractor = DataExtractor() # calls the DataExtractor class
user_df = data_extractor.read_rds_table('legacy_users')
print(user_df.head())
# user_df.to_csv('.legacy_users.csv') # produce the .csv file 

    
