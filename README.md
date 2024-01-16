# Multinational Retail Data Centralisation

### Table of Contents
---
1. Project Description
1. Installation Instructions
1. Usage Instructions
1. File Structure
   - .gitignore
   - database_utils.py
   - data_extraction.py
   - data_processing.py
   - data_cleaning.py
   - main.ipynb
1. License Information
   
### Project Description
---
This project extracts data from various sources (SQL database, url links, via api-keys and s3 buckets) in differing formats (.pdf, .json, .csv). The extracted datas are compiled into individual pandas dataframes for processing and cleaning. Finally, the cleaned pandas dataframes are uploaded to designated tables in a PostgreSQL database called sales_data. All data is connected via the orders_table in sales_data.   



### Installation Instructions
---
1. Ensure you have Visual Studio Code and pgAdmin4 installed and make the sales_data database for the data to be uploaded to on your local machine
1. Download all files
1. Connect your VSCode to pgAdmin4 via your local pgAdmin4 credentials to gain access to sales_data; create a new connection on VSCode to complete this step
1. Run the code in the **main.ipynb** file



### Usage Instructions
---
Once sales_data has been connected and main.ipynb has been run, all cleaned data tables are available to view and interact with via SQL. 
Please note, the following instructions detail pgAdmin4 use only, as the primary SQL platform. 



1. Refresh your local databases by right-clicking on "Databases" in the left tab and selecting "Refresh"
1. You should now be able to view sales_data in the left tab under Databases
1. Select "sales_data" -> "Schemas" -> "Tables" to view the data tables
1. Run any necessary SQL commands on tables as per your team's task requirements



### File Structure
---
#### .gitignore

Contains all files containing the credentials required to extract and load the raw data.

#### database_utils.py

Class DatabaseConnector
This class connects and uploads the cleaned data to the sales_data database.

#### data_extraction.py

Class DataExtractor
This class extracts all raw data from the various sources where the data is initially stored. Converts the data into panda DataFrames for ease of processing, cleaning and human readability.

#### data_processing.py

Class DataProcessor
This class contains all of the methods which process the dataframes. These methods are all staticmethods for seamless usage across various DataFrames. 

#### data_cleaning.py

Class DataCleaning
This class inherits from DataProcessor and applies all cleaning methods to the relevant DataFrames. 

#### main.ipynb

This file extracts, cleans and uploads each DataFrame to sales_data.



### License Information
---
None. 
