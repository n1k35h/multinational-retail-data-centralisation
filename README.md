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




 
