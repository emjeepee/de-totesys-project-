
# Project Title

ETL pipeline 



## Description of project

This project converts updated data from a transactional (OLTP) database called ToteSys, converts it into denormalised data in the form of dimensions tables and a facts table and puts this data in an Amazon RDS postgresql data warehouse. 



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








## Authors

 - Duncan Cornish (github.com/duncancornish)
 - Neill Hallard (github.com/nhallard)
 - Abdulmomen Jameli (github.com/Farctated)
 - Mukund Pandit (github.com/emjeepee)
 - Amar Walji (github.com/AvengedA)




## Installation

Setup instructions: fork this GitHub repository: https://github.com/AvengedA/de-totesys-project- and run file workflow.yml, which is in the .github directory.

    
## Tech Stack

 - CI/CD: GitHub Actions
 - Code: Python 3.13
 - Python modules pg8000.native, pandas, pyarrow, io, botocore, datetime, os, logging, json, dotenv, currency_codes, decimal
 - Testing: Python modules Pytest, coverage, unittest, moto 
 - Provisioning clud services: Terraform
 - Cloud services: AWS services S3, Lambda, EventBridge and CloudWatch


## What this project provisions
 - An AWS EventBridge scheduler
 - Three AWS lambda functions
 - Three AWS S3 buckets
 - EventBridge notifications
 - CloudWatch logging



## Operation of the project:
 - 1) An AWS EventBridge scheduler runs every five minutes and triggers the first of the 
	three lambda functions
 - 2) The first lambda function reads data in the ToteSys database and converts it into json 
	form before storing it in an S3 bucket called the ingestion bucket.
	The first lambda function saves complete tables in the ingestion bucket under a 
	key that includes a timestamp.
	On its very first run the first lambda function saves each table and all of its rows 
	in the postGresql ToteSys database as json data in the ingestion S3 bucket. The key 
	under the lambda saves each table contains a timestamp. 
	On subsequent runs the first lambda function only receives updated rows for tables.
	Here the lambda gets the appropriate table from the ingestion bucket and inserts the 
	updated rows into that table. The lambda then saves the whole table as json in the 
	ingestion bucket as json data under a key that contains a timestamp.
 - 3) When the ingestion S3 bucket has a new object put in it it generates and EventBridge 
	event that triggers the second lambda.
 - 4) When the second lambda is triggered it reads the new data that has just been put in 
	the ingestion bucket, converts it into dimension table and fact table data, converts 
	the tables into Parquet files and saves the Parquet files in the processed bucket.
 - 5) Putting objects into the processed S3 bucket makes the bucket send and EventBridge
	event to the third lamda, triggering it.
 - 6) The third lambda takes the new Parquet files out of the processed bucket, makes 
	SQL INSERT queries out of the data in them and makes the queries to the 
	appropriate tables in the data warehouse.	







## Running Tests

To run tests, run the following command after having navigated to the project directory

```bash
	pytest -vvvrP
```
