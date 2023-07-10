# multinational-retail-data-centralisation

Scenario for this Project is to work for Multinational Retail company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team. In an effort to become more data-driven, the organisation will make the sales data accessible from one centralised location. The first goal is to produce a system that stores the current company data in a database so that it's accessed from one centralised location and act as a single source of truth for sales data. The system will then query the database to get the up-to-date metrics for the business.

The tools and languages that this project will be using are as follows:
* Tools Used:
  * AWS - Amazon Web Services (Cloud technology)
  	* Extract data source files (e.g: API, S3 Bucket, JSON)
  * VSC - Visual Studio Code
  	* writing the python code
  * pgAdmin 4 - PostgreSQL 
  	* connect to the SQL database for the use of table creation and queries

* Languages Used:
  * Python
  * Pandas
  * Numpy
  * SQL

* Initilised 3 project classes with assigned modules in each classes:    
    * DatabaseConnector - connecting and uploading to the database

      	Module:
    	* read_db_file
     	* init_db_engine
      	* list_db_table
      	* upload_to_db
    * DataExtractor - extracting data from various data sources

		Module: 
    	* read_rds_table
     	* retrieve_pdf_data
      	* list_number_of_stores
      	* retrieve_stores_data
      	* extract_from_s3
      	* extract_from_s3_json
    * DataCleaning - cleaning data from each of the data sources
		
  		Module:
      	* clean_user_data
      	* clean_card_data
      	* clean_store_data
      	* convert_product_weights
      	* clean_products_data
      	* clean_orders_data
      	* clean_date_details

# Database
Setted up a Sales_Data database within the PGAdmin4, where it will store all the company information after the data has been extracted from various data sources.

Creating and reading the .yaml file within the read_db_file method

    def read_db_creds(self):
        # read the credentials yaml file and returns a dictionary of the credentials
        with open('db_creds.yaml', 'r') as f:
            creds_data = yaml.safe_load(f)
        return creds_data

Creating init_db_engine method and reading the credentials from the return of read_db_file

    def init_db_engine(self):
        # this method will read the credentials from the return of read_db_creds method and initialise and
        # return an sqlalchemy database engine
        creds_data = self.read_db_creds() # reads the credentials
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds_data['RDS_USER']}:{creds_data['RDS_PASSWORD']}@{creds_data['RDS_HOST']}:{creds_data['RDS_PORT']}/{creds_data['RDS_DATABASE']}")
        engine.connect()
        return engine

Using the engine method the list_db_tables method is created

    def list_db_tables(self):
        # this method will list all tables in the database so that the data can be extract from the relevent tables
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_name = inspector.get_table_names()
        return table_name

Creating the upload_to_db method to connect and upload the cleaned data to the table in the Sales_Data database 

        localengine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        localengine.connect()
        df.to_sql(name=table_name, con=localengine, if_exists='replace')

List of tables that the cleaned data will be uploaded too after the data is extracted from various data sources

    * dim_users
    * dim_store_details
    * dim_products
    * orders_table
    * dim_date_times

# Extracted the data from the data source
Extracting the users data from a table

    def read_rds_table(self, table_name):
        conn = DatabaseConnector() 
        engine = conn.init_db_engine()
        users_data = pd.read_sql_table(table_name, engine)
        return users_data

Extracting data from the PDF document

    def retrieve_pdf_data(self, pdf_data):
        t.convert_into(pdf_data, "card_details.csv", output_format= "csv", pages= "all")
        card_df = pd.read_csv("card_details.csv")
        return card_df

Extracting data from API

    def list_number_of_stores(self, get_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'):
        api_dict= {'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}        

        store = r.get(get_endpoint, headers=api_dict)
        store.status_code
        number_of_stores = store.json()
        
        return number_of_stores

    def retrieve_stores_data(self, retrieve_endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
        api_dict_list = []
        api_dict={'x-api-key' : 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

        for n in range(451):
            
            response = r.get(f'{retrieve_endpoint}/{n}', headers=api_dict)
            data = response.json()
            api_dict_list.append(data)
        
        response = pd.DataFrame.from_dict(api_dict_list)
		
        return response

Extracting data from S3 bucket in AWS source

CSV

    def extract_from_s3(self):
        product_df = pd.read_csv('s3://data-handling-public/products.csv')
        product_df.to_csv('products.csv')
        return product_df

JSON

    def extract_from_s3_json(self):
        date_df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        date_df.to_csv('date_details.csv')
        return date_df
  
# Cleaned the Data from the Extracted data source
Cleaning the users data

    def clean_user_data(self, users_df):
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
        # Adding country extensions to the front of phone numbers
        users_df["phone_number"] = users_df["country_code"].map({"DE": "0049 ", "GB": "0044 ", "US": "001 "}).astype(str) + users_df["phone_number"]

        # removes the duplicates of the email address
        users_df = users_df.drop_duplicates(subset=['email_address'])
        
        # removes the column 0 from the table
        users_df.drop(users_df.columns[0], axis=1, inplace=True)   
        
        return users_df
     
Cleaning PDF data 

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
        
        # stores large data type in the cell
        card_df['card_number'] = card_df['card_number'].astype('int64')
        
        return card_df

Cleaning API data

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

        # correcting the continent column by removing ee in front of Europe and America
        store_df['continent'] = store_df['continent'].str.replace('eeEurope', 'Europe').str.replace('eeAmerica','America')

        # re-arrange columns
        cols = ['address', 'locality', 'country_code', 'continent', 'store_code', 'store_type', 'longitude', 'latitude', 'opening_date', 'staff_numbers']
        store_df = store_df.reindex(columns = cols)   

        return store_df

Cleaning AWS S3 data from a .csv file

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
        product_df['weight'] = product_df['weight'].astype('float')

        product_df.drop(['amount', 'unit'], axis=1, inplace=True)

        return product_df        

    def clean_products_data(self, product_df):

        # Removes the Null value 
        product_df.replace('NULL', np.NaN, inplace=True)

        # correcting the date values and dropping the blank cells
        product_df['date_added'] = pd.to_datetime(product_df['date_added'], infer_datetime_format=True, errors='coerce')
        product_df.dropna(subset=['date_added'], how='any', axis=0, inplace=True)

        product_df['EAN'] = product_df['EAN'].astype('int64')

        # dropping the 'unnamed: 0' column as it's duplicating the index column
        product_df.drop(['Unnamed: 0'], axis=1, inplace=True) 

        return product_df

Cleaning JSON file

    def clean_date_details(self, date_df):

        # cleaning invalid entries         
        date_df['day'] = pd.to_numeric(date_df['day'], errors='coerce')
        date_df.dropna(subset=['day'], how='any', axis=0, inplace=True)

        # reorganise the columns
        date_cols = ['day', 'month', 'year', 'timestamp', 'time_period', 'date_uuid']
        date_df = date_df.reindex(columns = date_cols)
        
        return date_df

# Database Scheme
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

  
# Business Scenarios Queries

1. How many stores does the business have and in which countries?

The Operations team would like to know which countries we currently operating in and which country now has the most stores.

	SELECT country_code AS country,
	COUNT(store_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY country
	ORDER BY total_no_stores DESC;

2. Which locations currently have the most stores?

The business stakeholders would like to know which locations currently have the most stores.

	SELECT locality,
	COUNT(store_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY locality
	ORDER BY total_no_stores DESC
	LIMIT 7;

3. Which months produce the average highest cost of sales typically?

Query the database to find out which months typically have the most sales your query should return the following information:

	WITH highest_avg_monthly_sales AS(
		SELECT *
			FROM dim_date_times AS ddt
		INNER JOIN
			orders_table AS ot ON ot.date_uuid = ddt.date_uuid
		INNER JOIN 
			dim_products AS dp ON dp.product_code = ot.product_code)
			SELECT			
				ROUND(CAST(SUM(product_quantity*product_price)AS NUMERIC), 2) AS total_sales,
				month,
				ROUND(CAST(AVG(product_quantity*product_price)AS NUMERIC), 2) AS avg_per_month
			FROM highest_avg_monthly_sales
			GROUP BY month
			ORDER BY total_sales DESC;

4. How many sales are coming from online?

The company is looking to increase its online sales. They want to know how many sales are happening online vs offline.

	ALTER TABLE dim_store_details
		ADD location VARCHAR(11);
	
	UPDATE dim_store_details
		SET location = 'Web'
		WHERE store_type = 'Web Portal';
	
	UPDATE dim_store_details
		SET location = 'Offline'
		WHERE store_type != 'Web Portal';
	
	WITH online_sales AS(
		SELECT * 
			FROM dim_store_details AS dsd
			INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code)
			SELECT COUNT(date_uuid) AS number_of_sales,
			SUM(product_quantity) AS product_quantity_count,
				location
			FROM online_sales
			GROUP BY location
			ORDER BY number_of_sales DESC;

5. What percentage of sales come through each type of store?

The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

	WITH sales_order AS (
		SELECT *
		FROM dim_store_details AS dsd
		INNER JOIN orders_table AS ot ON ot.store_code = dsd.store_code
		INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code)
			SELECT store_type,
				ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
				ROUND(CAST(SUM(product_price * product_quantity) * 100.00 / 
				(SELECT SUM(product_price * product_quantity) FROM sales_order)AS NUMERIC), 2) AS percentage_total
			FROM sales_order
			GROUP BY store_type
			ORDER BY percentage_total DESC;

6. Which month in each year produced the highest cost of sales?

The company stakeholders want assurances that the company has been doing well recently.

Find which months in which years have had the most sales historically.

	WITH highest_month_sales_per_year AS(
		SELECT * 
		FROM orders_table AS ot
		INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
		INNER JOIN dim_date_times AS ddt ON ddt.date_uuid = ot.date_uuid)
			SELECT 
				ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC),2) AS total_sales,
				year,
				month
			FROM highest_month_sales_per_year
			GROUP BY year, month
			ORDER BY total_sales DESC;

7. What is our staff headcount?

The operations team would like to know the overall staff numbers in each location around the world.

Perform a query to determine the staff numbers in each of the countries the company sells in.

	SELECT SUM(staff_numbers) AS total_staff_numbers,
		country_code
		FROM dim_store_details
		GROUP BY country_code
		ORDER BY total_staff_numbers DESC;

8. Which German store type is selling the most?

The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.

	WITH german_store_sales AS (
		SELECT *
		FROM orders_table AS ot
		INNER JOIN dim_products AS dp ON dp.product_code = ot.product_code
		INNER JOIN dim_store_details AS dsd ON dsd.store_code = ot.store_code)
			SELECT 
				ROUND(CAST(SUM(product_price * product_quantity)AS NUMERIC), 2) AS total_sales,
				store_type,
				country_code
			FROM german_store_sales
			WHERE country_code = 'DE'
			GROUP BY store_type, country_code
			ORDER BY total_sales ASC;

9. How quickly is the company making sales?

Sales would like to get the accurate metric for how quickly the company is making sales.

Determine the average time taken between each sale grouped by year

	WITH company_make_sales AS(
		SELECT TO_TIMESTAMP(CONCAT(day, '-', month, '-', year, ' ', timestamp), 'DD-MM-YYYY HH24:MI:SS') AS datetime,
		year
		FROM dim_date_times
		ORDER BY datetime DESC),
		company_make_sales_2 AS(
			SELECT 
				year,
				datetime,
				LEAD(datetime, 1) OVER (ORDER BY datetime DESC) AS time_elapse FROM company_make_sales)
				SELECT
					year,
					AVG((datetime - time_elapse)) AS actual_time_taken FROM company_make_sales_2
				GROUP BY year
				ORDER BY actual_time_taken DESC 
				limit 5;
