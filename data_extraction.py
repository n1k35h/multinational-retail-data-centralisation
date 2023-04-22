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
    
    def retrieve_pdf_data(self, pdf_data):
        t.convert_into(pdf_data, "card_details.csv", output_format= "csv", pages= "all")
        df = pd.read_csv("card_details.csv")
        return df

    def list_number_of_stores(self, get_store_nums = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):

        store = r.get(get_store_nums, headers=api_dict)
        store.status_code
        number_of_stores = store.json()
        
        # print(number_of_stores)
        return number_of_stores

    def retrieve_stores_data(self, store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):

        api_dict_list = []
        # store_nums = self.list_number_of_stores()

        for n in range(451):
            # if n%25 == 0:
            #     print(n, "/", store_nums)
            response = r.get(f'{store_endpoint}/{n}', headers=api_dict)
            data = response.json()
            api_dict_list.append(data)
        
        response = pd.DataFrame.from_dict(api_dict_list)
        # response.to_csv('store_details.csv')
        
        # print(response)
        return response
    
    def extract_from_s3(self):
        
        df = pd.read_csv('s3://data-handling-public/products.csv')
        df.to_csv('products.csv')
        return df    

extractor = DataExtractor() # calls the DataExtractor class
user_df = extractor.read_rds_table('legacy_users')
# print(user_df.head())
user_df.to_csv('legacy_users.csv') # produce the .csv file 

# link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# df = de.retrieve_pdf_data(link)
# print(df.head())

# # de.retrieve_stores_data()
# retrieve = de.retrieve_stores_data()
# print(retrieve.head())
# retrieve.to_csv('store_details.csv')

product_df = de.extract_from_s3()
print(product_df.head())
    
