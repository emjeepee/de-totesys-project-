
# Project Title

### **Extract-transform-load (ETL) pipeline** <br><br><br>


## Description of project
A project that transforms data in a transactional OLTP database into star-schema data for analytics.  
This project converts updated data from OLTP database ToteSys into denormalised data in the form of dimensions tables and a facts table and puts this data in an Amazon RDS postgresql data warehouse. 
 <br><br><br>


## Project directories
This project includes the following directories:
 - .github -- contains the GitHub Actions workflow.yml file
 - src -- contains all the python files for the three lambda functions.
		This includes a main python file for each lambda and separate files 
		for the utility functions employed by the lambda functions
 - terraform -- this directory contains all Terraform *.tf files that describe 
		the provisioning of AWS cloud services this project needs.
		NOTE: the Terraform state must be placed elsewhere.
 - tests -- this directory contains test_*.py files, which contain the pytest
		functions that will test all of the python code that makes up the 
		lambda functions
 <br><br><br>



## Authors (alphabetical)

- Mukund Pandit (created everything except the workflow.yml file for GitHub Actions)
- Neill Hallard (created the workflow.yml file for GitHub Actions)

 <br><br><br>



## Installation

Setup instructions:  <br>
fork this GitHub repository: https://github.com/emjeepee/de-totesys-project- and run file workflow.yml, which is in directory .github.
 <br><br><br>
    
## Tech Stack

 - CI/CD: GitHub Actions
 - Code: Python 3.13
 - Python libraries pg8000, pg8000.native, pandas, pyarrow, io, botocore, datetime, os, logging, json, dotenv, currency_codes, calendar and decimal
 - Testing: Python libraries Pytest, coverage, unittest and moto 
 - Provisioning of cloud services: Terraform
 - AWS cloud services: S3, Lambda, EventBridge and CloudWatch
 <br><br><br>



## What this project provisions
 - An AWS EventBridge scheduler
 - Three AWS lambda functions
 - Two AWS S3 buckets
 - AWS EventBridge notifications
 - AWS CloudWatch logging
 <br><br><br>


## Operation of the project:
### **Overview** <br>
***This project:***
 - 		extracts updated data from postgresql database ToteSys, 
 		which holds the data in several tables. 
 - 		transforms each updated table into either a dimension table 
		or the single fact table.
 - 		converts each dimension table or the single fact table into a 
		Parquet file.
 - 		converts the Parquet file into a Pandas DataFrame.   
 - 		converts each DataFrame into SQL queries and, using those 
		queries, inserts the table's rows into a data warehouse (an
		AWS RDS postgresql database). 		
 <br>

### **In detail** <br>
***This project:***
 -  	Has an AWS EventBridge scheduler run every five minutes to 
 	  	trigger the first of the three lambda functions.
 -  	The first lambda function polls ToteSys for updated row data 
 	  	for each table, creates an updated table and stores it in 
	  	json form in the ingestion bucket, the first S3 bucket, where 
	  	the previous, now-outdated, table also remains.
	  	The first lambda function saves each table in the bucket under 
	  	a key string comprising two parts: the table name, eg 'design', 
	  	and a timestamp.
 -  	On its very first run the first lambda function receives all of 
      	the rows of a table.
 -  	When the first lambda function writes a table to the ingestion 
 	  	bucket, AWS generates an EventBridge event that triggers the 
	  	second lambda function.
 -  	The second lambda function reads the new table file that has 
 	  	just entered the ingestion bucket, converts it into a dimension 
	  	table or a fact table and converts the dimension/fact table 
	  	into a Parquet file. 
	  	The second lambda function then saves the Parquet file in the 
	  	second S3 bucket, the processed bucket, replacing the Parquet 
	  	file for the same table that is already there. 
	  	Writing the file to the bucket triggers an AWS EventBridge 
	  	event that reaches the third lambda function.
 -  	The third lambda function responds to the event by reading the 
 	  	new Parquet file in the processed bucket and converting it to 
	  	a pandas DataFrame.
	  	The third lambda function reads the DataFrame and forms 
		SQL	INSERT query strings out of the row data in it, then makes 
	  	those queries to the appropriate table in the data warehouse.	
	  
 <br><br><br>



# Running Tests

To run the tests, execute the following in the command line after having navigated to the project directory:

```bash
	pytest -vvvrP
```
