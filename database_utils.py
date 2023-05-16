import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
# import pandas as pd


# class DatabaseConnector will use to connect with and upload data to the database
class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        # read the credentials yaml file and returns a dictionary of the credentials
        with open('db_creds.yaml', 'r') as f:
            creds_data = yaml.safe_load(f)
        return creds_data

        # print(data)

        
    def init_db_engine(self):
        # this method will read the credentials from the return of read_db_creds method and initialise and
        # return an sqlalchemy database engine
        creds_data = self.read_db_creds() # reads the credentials
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds_data['RDS_USER']}:{creds_data['RDS_PASSWORD']}@{creds_data['RDS_HOST']}:{creds_data['RDS_PORT']}/{creds_data['RDS_DATABASE']}")
        engine.connect()
        return engine

    def list_db_tables(self):
        # this method will list all tables in the database so that the data can be extract from the relevent tables
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_name = inspector.get_table_names()
        return table_name

    def upload_to_db(self, df, table_name,):
        # takes pandas DataFrame and table name to upload to as an argument
        # use the upload_to_db method to store the data in your sales_data database
        # in a table named dim_users
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = '5c008yd00d00'
        PORT = 5432
        DATABASE = 'Sales_Data'

        localengine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        localengine.connect()
        df.to_sql(name=table_name, con=localengine, if_exists='replace')
        print('Table Uploaded')

conn = DatabaseConnector()
