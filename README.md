# multinational-retail-data-centralisation

Scenario for this Project is to work for Multinational Retail company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation will make the sales data accessible from one centralised location. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and act as a single source of truth for sales data. The system will then query the database to get the up-to-date metrics for the business.

# Milestone 1:
* Setting up the develope environment to get started

# Milestone 2: Extracted and Cleaned the data from the data source
* Setted up a Sales_Data database within the PGAdmin4, where it will store all the company information after the data has been extracted from various data sources.

* Initilised 3 project classes:
    * DataExtractor - extracting data from various data sources
    * DatabaseConnector - connecting and uploading to the database
    * DataCleaning - cleaning data from each of the data sources

* Creating and reading the .yaml file within the read_db_file method

       def read_db_creds(self):
         with open('db_creds.yaml', 'r') as f:
            creds_data = yaml.safe_load(f)
        return creds_data

* Creating init_db_engine method and reading the credentials from the return of read_db_file

       def init_db_engine(self):
           creds_data = self.read_db_creds() # reads the credentials
           engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds_data['RDS_USER']}:{creds_data['RDS_PASSWORD']}@{creds_data['RDS_HOST']}:{creds_data['RDS_PORT']}/{creds_data['RDS_DATABASE']}")
           engine.connect()
           return engine

* Using the engine method the list_db_tables method is created

		 def list_db_tables(self):
			  engine = self.init_db_engine()
			  inspector = inspect(engine)
			  table_name = inspector.get_table_names()
			  return table_name

* Creating the upload_to_db method to connect and upload the cleaned data to the table in the Sales_Data database 

	    def upload_to_db(self, df, table_name,):
		DATABASE_TYPE = 'postgresql'
		DBAPI = 'psycopg2'
		HOST = 'localhost'
		USER = 'postgres'
		PASSWORD = #password
		PORT = 5432
		DATABASE = 'Sales_Data'

		localengine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
		localengine.connect()
		df.to_sql(name=table_name, con=localengine, if_exists='replace')


* list of tables that the cleaned data will be uploaded too after the data is extracted from various data sources

    * dim_users
    * dim_card_details
    * dim_store_details
    * dim_products
    * orders_table
    * dim_date_times

* Extracting the users data from a table

		 def read_rds_table(self, table_name):
			  conn = DatabaseConnector() 
			  engine = conn.init_db_engine()
			  users_data = pd.read_sql_table(table_name, engine)
			  return users_data

* Cleaning the users data

		 def clean_user_data(self, users_df):
			  users_df.replace('NULL', np.NaN, inplace=True)
			  users_df.dropna(subset=['date_of_birth', 'email_address', 'user_uuid'], how='any', axis=0, inplace=True)

			  # removes invalid dates from the Table
			  users_df['date_of_birth'] = pd.to_datetime(users_df['date_of_birth'], infer_datetime_format=True, errors = 'coerce')
			  users_df['join_date'] = pd.to_datetime(users_df['join_date'], infer_datetime_format=True, errors ='coerce')
			  users_df = users_df.dropna(subset=['join_date'])

			  # # changes the GGB to GB for the country code
			  users_df['country_code'] = users_df['country_code'].str.replace("GGB", "GB")

			  # removing invalid characters from the phone number
			  r = "[^0-9]+."
			  users_df["phone_number"] = users_df["phone_number"].str.replace(r, "")
			  #Adding country extensions to the front of phone numbers
			  users_df["phone_number"] = users_df["country_code"].map({"DE": "0049 ", "GB": "0044 ", "US": "001 "}).astype(str) + users_df["phone_number"]

			  users_df = users_df.drop_duplicates(subset=['email_address'])
			  users_df.drop(users_df.columns[0], axis=1, inplace=True)   

			  return users_df

* Extracting data from the PDF document

		 def retrieve_pdf_data(self, pdf_data):
			  t.convert_into(pdf_data, "card_details.csv", output_format= "csv", pages= "all")
			  card_df = pd.read_csv("card_details.csv")
			  return card_df

* Cleaning PDF data 

		 def clean_card_data(self, card_df):
			  # format data
			  # removes the null values
			  card_df.replace('NULL', np.NaN, inplace=True)
			  card_df.dropna(subset=['card_number'], how='any', axis=0, inplace=True)

			  # not containing alphabet and removes empty cells/ rows
			  r = "[a-zA-Z?]"
			  card_df = card_df[~card_df['card_number'].str.contains(r, na=False)]

			  # correct the dates
			  card_df['date_payment_confirmed'] = pd.to_datetime(card_df['date_payment_confirmed'], infer_datetime_format=True, errors = 'coerce')

			  # # stores large data type in the cell
			  card_df['card_number'] = card_df['card_number'].astype('int64')

        	  return card_df
			  
* Extracting data from API 

		 def list_number_of_stores(self, get_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
			  api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
			  store = r.get(get_endpoint, headers=api_dict)
			  store.status_code
			  number_of_stores = store.json()
			  return number_of_stores

		 def retrieve_stores_data(self, retrieve_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
			  api_dict_list = []
			  api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
			  for n in range(451):
					response = r.get(f'{retrieve_endpoint}/{n}', headers=api_dict)
					data = response.json()
					api_dict_list.append(data)
			  response = pd.DataFrame.from_dict(api_dict_list)
			  return response

* Cleaning API data

		 def clean_store_data(self, store_df):

			  # remove the 'N/A' from the first row
			  store_df.replace('N/A', np.NaN, inplace=True)
			  store_df[['staff_numbers', 'longitude', 'latitude']] = store_df[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))
			  store_df.dropna(subset=['staff_numbers'], inplace=True)
			  store_df.astype({'staff_numbers' : 'int64'})

			  # dropping the 'Lat' column as there is no value in the 'Lat'
			  store_df.drop(['lat'], axis = 1, inplace=True)

			  # correcting the date 
			  store_df['opening_date'] = pd.to_datetime(store_df['opening_date'], infer_datetime_format=True, errors='coerce')

			  # re-arrange columns
			  cols = ['address', 'locality', 'country_code', 'continent', 'store_code', 'store_type', 'longitude', 'latitude', 'opening_date', 'staff_numbers']
			  store_df = store_df.reindex(columns = cols)   

			  return store_df



* Extracting .csv data from S3 bucket in AWS source

		 def extract_from_s3(self):

			  product_df = pd.read_csv('s3://data-handling-public/products.csv')
			  product_df.to_csv('products.csv')
			  return product_df

* Cleaning AWS S3 data from a .csv file

		 def convert_product_weight(self, product_df):

			  #   removes the non-digits from the weight column - kg, g, oz, ml
			  product_df['amount'] = product_df['weight'].str.replace(r'[^0-9.]+', '')

			  # creates new column for weight amount
			  product_df['amount'] = pd.to_numeric(product_df['amount'])
			  product_df['amount'] = product_df['amount'].astype('float')

			  # create new column for weight unit
			  product_df['unit'] = product_df.weight.str.replace('[^a-zA-Z]', '')
			  product_df['unit'] = product_df['unit'].astype('str')

			  # converting weight to kg
			  product_df.loc[product_df['unit'] == 'g', 'amount'] /= 1000
			  product_df.loc[product_df['unit'] == 'ml', 'amount'] /= 1000
			  product_df.loc[product_df['unit'] == 'oz', 'amount'] *= 28.34952

			  # replace weight column and remove extra columns
			  product_df['weight'] = product_df['amount']
			  product_df.rename(columns={'weight':'weight(kg)'}, inplace=True)
			  product_df['weight(kg)'] = product_df['weight(kg)'].astype('float')

			  product_df.drop(['amount', 'unit'], axis=1, inplace=True)

			  return product_df        

		 def clean_products_data(self, product_df):

			  # Removes the Null value 
			  product_df.replace('NULL', np.NaN, inplace=True)

			  # correcting the date values and dropping the blank cells
			  product_df['date_added'] = pd.to_datetime(product_df['date_added'], infer_datetime_format=True, errors='coerce')
			  product_df.dropna(subset=['date_added'], how='any', axis=0, inplace=True)

			  # removing unwanted character in the product_price
			  product_df['product_price'] = product_df['product_price'].str.replace('[^0-9.]+', '')
			  product_df['product_price'] = product_df['product_price'].astype('float')

			  product_df['EAN'] = product_df['EAN'].astype('int64')

			  # dropping the 'unnamed: 0' column as it's duplicating the index column
			  product_df.drop(['Unnamed: 0'], axis=1, inplace=True) 

			  return product_df

* Extracting JSON file 

		 def extract_from_s3_json(self):

			  date_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
			  date_df.to_csv('date_details.csv')
			  return date_df

* Cleaning JSON file

		 def clean_date_details(self, date_df):

			  # cleaning invalid entries         
			  date_df['day'] = pd.to_numeric(date_df['day'], errors='coerce')
			  date_df.dropna(subset=['day'], how='any', axis=0, inplace=True)

			  # reorganise the columns
			  date_cols = ['day', 'month', 'year', 'timestamp', 'time_period', 'date_uuid']
			  date_df = date_df.reindex(columns = date_cols)

			  return date_df

# Milestone 3: Database Scheme
Developing the star-based schema of the database to ensure that the data type for the required columns of each table is correctly assigned. For example: 

            ALTER TABLE orders_table
                ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid as UUID),
                ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid as UUID),
                ALTER COLUMN card_number TYPE VARCHAR(20),
                ALTER COLUMN store_code TYPE VARCHAR(12),
                ALTER COLUMN product_code TYPE VARCHAR(11),
                ALTER COLUMN product_quantity TYPE SMALLINT
                ;
                
            SELECT * FROM orders_table;


New column weight_class is added to the dim_products table to improve human-readability to allow quick decision-making on delivery weights.

            SELECT *,
                (CASE
                    WHEN weight < 2.0 THEN 'Light'
                    WHEN weight >= 2.0 AND weight < 40.0 THEN 'Mid_Sized'
                    WHEN weight >= 40.0 AND weight < 140.0 THEN 'Heavy'
                    WHEN weight >= 140.0 THEN 'Truck_Required'
                END) AS weight_class
                FROM dim_products
                ORDER BY index asc;

Completing the final stages of the star-based database schema - Primary Keys is added to the relevant columns to each of the 5 tables

            /* created the primary key for each of the dim tables */

            ALTER TABLE dim_users
                ADD PRIMARY KEY (user_uuid);

            ALTER TABLE dim_store_details
                ADD PRIMARY KEY (store_code);

            ALTER TABLE dim_products
                ADD PRIMARY KEY (product_code);

            ALTER TABLE dim_date_times
                ADD PRIMARY KEY (date_uuid);

            ALTER TABLE dim_card_details
                ADD PRIMARY KEY (card_number);

Below code is required to add data that are in orders_table but not in dim_store_details, which are now added to the dim_store_details.

            SELECT orders_table.store_code
                FROM orders_table
                LEFT JOIN dim_store_details
                ON orders_table.store_code = dim_store_details.store_code
                WHERE dim_store_details.store_code IS NULL;

            INSERT INTO dim_store_details(store_code)
                SELECT DISTINCT orders_table.store_code
                FROM orders_table
                WHERE orders_table.store_code NOT IN 
                    (SELECT dim_store_details.store_code
                    FROM dim_store_details);

Foreign Keys are added to the orders_table to get the relations between the orders_table to the other 5 dim tables.

            ALTER TABLE orders_table
                ADD FOREIGN KEY (user_uuid)
                REFERENCES dim_users(user_uuid);

            ALTER TABLE orders_table
                ADD FOREIGN KEY (store_code)
                REFERENCES dim_store_details;

            ALTER TABLE orders_table
                ADD FOREIGN KEY (product_code)
                REFERENCES dim_products;

            ALTER TABLE orders_table
                ADD FOREIGN KEY (date_uuid)
                REFERENCES dim_date_times;
                
            ALTER TABLE orders_table
                ADD FOREIGN KEY (card_number)
                REFERENCES dim_card_details;

