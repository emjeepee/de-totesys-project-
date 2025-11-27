
### Project Title

## **Extract-transform-load (ETL) pipeline** <br><br><br>


## Description of project
This project creates an extract-transform-load (ETL) pipeline that reads table data from an OLTP database, transforms the data so that it is compatible with the star-schema model and writes the data to an OLAP data warehouse.  

 <br><br><br>


## Project directories
The most important directories in this project are:
 - .github -- contains the GitHub Actions workflow.yml file
 - src -- contains all the python modules for this project.
		This includes a python module that contains three lambda handlers. It also contains utility modules that the lambda handlers employ.
 - terraform -- this directory contains all the Terraform *.tf files that together provision the cloud services that this project employs. This project employs three AWS Lambda functions, two S3 buckets, AWS CloudWatch, an AWS EventBridge scheduler and AWS EventBridge notifications
 - tests -- contains test_*.py files, which contain the pytest functions that will test all of the python code that makes up the lambda handlers and their utility functions.
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
 - AWS cloud services: S3, Lambda, EventBridge and CloudWatch
 - Code: Python 3.12
 - CI/CD: GitHub Actions
 - Python libraries botocore, calendar, copy, currency_codes, datetime, decimal dotenv, io, json, logging, os, pg8000.native, tempfile 
 - Testing: Python libraries coverage, moto, Pytest and unittest

 
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
This project converts data from an OLTP database ToteSys into denormalised data in the form of dimensions tables and a fact table and puts this data into an AWS RDS postgresql data warehouse. 

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
