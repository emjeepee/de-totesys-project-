
### Project Title
## **Extract-transform-load (ETL) pipeline** <br><br><br>


## Description of project
This project creates an extract-transform-load (ETL) pipeline. <br> <br>
The pipeline  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; - reads table data from a postgresql online transaction processing (OLTP) database  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; - transforms the data so that it is compatible with the star-schema model   <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; - writes the data to a postgresql online analytical processing (OLAP) data warehouse. 

 <br><br><br>


## Project directories
The most important directories in this project are:
 - **.github** -- contains the GitHub Actions workflow.yml file <br><br>
 - **src** -- contains these directories: <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;first_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; directory first_lambda_utils -- which contains utility functions that the first lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; python module first_lambda_handler.py, which contains the code for the first lambda handler.  <br>


&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;second_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; directory second_lambda_utils -- which contains utility functions that the second lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; python module second_lambda_handler.py, which contains the code for the second lambda handler.  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;third_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; directory third_lambda_utils -- which contains utility functions that the third lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; python module third_lambda_handler.py, which contains the code for the third lambda handler. <br><br>

 - **terraform** -- this directory contains all the Terraform *.tf files that together provision the cloud services that this project employs. <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;This project employs three AWS Lambda functions, two S3 buckets, AWS CloudWatch, an AWS EventBridge scheduler and AWS EventBridge notifications.  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;See the separate README.md file in this directory for details about how this project employs infrastructure as code.<br><br>

 - **tests** -- contains test_*.py files, which contain the pytest functions that tests all of the python code that makes up the lambda handlers and their utility functions.
 <br><br><br>



## Alphabetical list of authors

- Neill Hallard (created the workflow.yml file for GitHub Actions) <br>
- Mukund Pandit (created everything except the workflow.yml file for GitHub Actions) <br>


 <br><br><br>



## Installation

Setup instructions:  <br>
fork this GitHub repository: https://github.com/emjeepee/de-totesys-project- and run file workflow.yml, which is in directory .github.
 <br><br><br>
    
## Tech Stack
 - AWS cloud services: S3, Lambda, EventBridge and CloudWatch <br>
 - Code: Python 3.12 <br>
 - CI/CD: GitHub Actions <br>
 - Python libraries botocore, calendar, copy, currency_codes, datetime, decimal dotenv, io, json, logging, os, pg8000.native, tempfile  <br>
 - Testing: Python libraries coverage, moto, Pytest and unittest <br>

 
 <br><br><br>



## What cloud infrastructure this project provisions
 - An AWS EventBridge scheduler
 - Three AWS Lambda functions labelled first lambda, second lambda and third lambda
 - Two AWS S3 buckets labelled the ingestion bucket and the processed bucket
 - AWS EventBridge notifications
 - AWS CloudWatch logging for each lambda function
 <br><br><br><br><br>


## Overview of the operation of the pipeline:
This project converts data from postgresql OLTP database totesys into denormalised data in the form of dimensions tables and a fact table, which this project writes into an AWS RDS postgresql data warehouse.  <br>

To carry this out the following occurs: <br>

**1)** An AWS EventBridge scheduler triggers the first lambda function every five minutes.  <br> <br> 
**2)** The first lambda function behaves differently depending on whether the pipeline is running <br>
<br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 	**a)** for the first time ever 
<br> <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 	**b)** for the 2nd-plus time 
<br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; **i)** If a) is true the first lambda function makes SQL queries to read all rows of all tables in OLTP database totesys.  <br>   <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; **ii)** If b) is true the first lambda function instead makes the queries to read only the updated rows of tables in database totesys.   <br>   <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; **iii)** In either case a) or b) the first lambda function converts each partial-table or whole-table data into a partial/whole table in the form of a Python list of dictionaries, where each dictionary represents a row and its key-value pairs are column-name: field-value pairs   <br>   <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; **iv)** If a) is true the first lambda function converts each whole table into json form and puts it in the ingestion bucket. If b) is true the first lambda function reads the ingestion bucket for the latest version of a table, replaces its rows with the rows that contain updated data, converts the table into json form and puts it in the ingestion bucket as a new table, leaving the original latest table unchanged   <br>     

**3)** The AWS S3 service responds to the writing of a table to the ingestion bucket by sending an event to the second lambda function, causing the second lambda function to run.   <br>

**4)** The second lambda:   <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; i) runs in response to the trigger from S3  <br>  <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; ii) gets the just-saved table from the ingestion bucket, transforms it into a dimension or fact table in the form of a list of dictionaries (where each dictionary represents a row and its key-value pairs are column-name: field-value pairs)    <br>  <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; iii) converts the table into a Parquet file and puts it into a BytesIO buffer   <br>  <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; iv) puts the buffer containing the Parquet dimension table or fact table in the processed bucket under a key of this form: "timestamp:table-name"    <br> 
<br> 

**5)** Writing a table to the processed bucket makes the AWS S3 service trigger an event to which the third lambda responds.   <br> 

**6)**  The third lambda:   <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; i) runs in response to the trigger from S3
<br> <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; ii) gets the just-saved buffer (that contains a table in Parquet format) from the processed bucket    
<br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; iii) converts the Parquet file into a DuckDB table in memory   
<br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; iv) employs DuckDB to read the table in the Parquet file into an in-memory DuckDB database   
<br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; v) employs DuckDB to create SQL query strings from each row of the table   
<br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; vi) uses the query strings to insert each row into the appropriate table in the OLAP data warehouse.   <br> 
 <br>

<br><br><br>







## Detailed description of the operation of the pipeline:
<br>

### Operation of the first lambda function   <br> <br>

Operation of the first lambda function
-------------------------------------------------
The first lambda function: 

1)
Checks the value of environment variable WHAT_ENV. If the value is “dev” then code employs library dotenv (which behind the scenes emloys os.environ) to load all environmnet variables from the .env file. If the value is “prod” (which it will be in AWS Lambda because the Lambda resource block of the Terraform child module’s main.tf file sets the value to “prod”)

2) Runs  lookup = get_env_vars(), which creates a lookup table that contains various values the first lambda hanler will employ, eg a boto3 S3 client object and a the names of the ingestion and processed buckets.


3) calls change_after_time_timestamp()
This tries to read the timestamp string stored in the ingestion bucket.
If it succeeds it returns that string.
If it fails it returns the default timestamps string, which represents the year 1900 ("1900-01-01T00-00-00")


4) The first lambda function then calls function get_data_from_db(),
which employs a loop in which it calls read_table() on every table name.

3) 
read_table(table, conn, after_time)behaves differently depending on the value of environment variable 
IS_OLTP_OK. This environment variable has value True for when database totesys contains useful data (pre-Nov2025) or False for when database totesys no longer contains useful data (post-Nov2025).   

When the value of  IS_OLTP_OK is True:
In the loop, read_table() does this for each table:
	i) calls get_column_names(conn, table_name), which makes an SQL 
	query to the totesys database to get the column names of the table in 
	question. 
	This function returns a lists of lists, each member list 
	containing one member, a string for a column name
 	ii) calls get_updated_rows(conn_obj, after_time, table_name), 
	which makes an SQL query to the totesys database to get the rows of the
	table in question that have been updated after time after_time. 
	This function returns a lists of lists, each member list representing a row and 
	containing field values. In the first ever run of the pipeline, this function   
	retrieves all of the rows of a table.
	iii) Changes the columns list of lists to a list of strings, like this:
	[‘xxx’_id, 'location', etc]
	iv) calls convert_values() to change cell value types like this:
	# datetime.datetime object -> ISO string
     # Decimal value            -> float
     # json                     -> string:
	v) calls make_row_dicts(clean_col_names, cleaned_rows)
	 to make a list of rows like this:
    	 [ 
   {"design_id": 6,  "name": "aaa", etc},         
   {"design_id": 7,  "name": "bbb", etc},
    #   etc ]
	and to return the list of rows in a dictionary as the value of a key that is the name of the table like this:
 { "sales_order”: # ‘sales_order’ is name of the table
	[  
{"Name": "xx", "Month": "January", “sales”_id”: 3, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 4, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 9, etc},#a row 
    etc
     ] 
 }
	vi) returns the dict above

When the value of  IS_OLTP_OK is True:
In the loop, read_table() creates fake tables and returns  dictionary  {table_name: row_list_of_dicts}

In the same loop get_data_from_db() calls clean_data(table, table_dict), which calls converts datetime.datetime objects
into iso strings and converts decimal.Decimal objects into strings (ie making the data json safe), and appends the cleaned dict (ie the dict in code 1 above) to a list. get_data_from_db() returns that list.
On the first ever run of the first lambda function the list will contain 
every table and every row of each table. 
On subsequent runs of the first lambda function the list will more likely contain 
less than all of the tables (only the ones containing data that database totesys has changed since the last run of this pipeline) and within those tables only the updated rows (so most likely not all of the rows):

get_data_from_db() returns a list like this:
	[ 
  {sales_orders: [{...}, {...}, etc}]},
  {design: 	  [{...}, {...}, etc}]},
	etc
	]



5) calls reorder_list(
                new_table_data, 
                "address", 
                "department")
where new_table_data is the list that get_data_from_db() returned.
reorder_list() puts the address table and department table at the top of the list if they are present in that list.

6) runs is_first_run = is_first_run_of_pipeline(). That function lists the objects in the processed bucket. If there are some this function returns false (meaning that this is the 2nd-plus run of the pipeline), if there are none (meaning this is the first ever run of the pipline), this function returns True.

7) If is_first_run is True the first lambda handler calls 
write_tables_to_ing_buck(lookup['s3_client'], 
                                lookup['ing_bucket_name'],
                                data_for_s3
                                ) to save the table in the ingestion bucket in json format.

Function write_tables_to_ing_buck() calls create_formatted_timestamp() to make a timestamp, then calls save_updated_table_to_S3() to put the table in the ingestion bucket under a key that includes the timestamp and in json format.

If is_first_run is False the first lambda handler calls 
make_updated_tables(data_for_s3, 
                        lookup['s3_client'], 
                        lookup['ing_bucket_name’]),
which calls get_most_recent_table_data(), which calls get_latest_table()

data_for_s3 contains representations of tables and their updated rows only.

For each table in data_for_s3, make_updated_tables() gets the lastest all-rows version of that table in the ingestion bucket and updates its rows with the rows in data_for_s3. make_updated_data() then returns a dictionary that looks like this:
          [
          {'design': [{row}, {row}, {row}, etc]},
          {'sales_order': [{row}, {row}, {row}, etc]},
          etc
          ]
The list that is the value of the key ‘design’, for example, contains all rows of the design table now containing updated rows. 

The first lambda handler then calls  write_tables_to_ing_buck() to save the each table in the list above to the ingestion bucket under a new key and as a jsonified dictionary. In this way the previous versions of each table remain in the ingestion bucket.
 
8) calls   lookup['close_db'](lookup['conn’]), which closes the connection to database totesys.

When the first lambda handler puts a new table (with updated rows) in the ingestion bucket, AWS S3 triggers the second lambda.


Operation of the second lambda function
------------------------------------------------------
second_lambda_handler(event, context) 
1) runs in response to an event sent by AWS S3 to the second lambda when the first lambda has put a table into the ingestion bucket (in the form of a .json file).

2) calls second_lambda_init() to create a lookup table in the form of a dictionary from which it can access values that the second lambda handler requires.

3) determines whether the table just put into the ingestion bucket is the department table. If so the second lambda handler stops running because it has no need to create a department dimension table.

4) if the table just put into the ingestion bucket is NOT the department table 
the first lambda handler calls read_from_s3() to retrieve that table from the ingestion bucket. The table is in the form of data in json format. The second lambda handler converts the table into a list.

5) calls is_first_run = is_first_run_of_pipeline(lookup['proc_bucket'],                                            lookup['s3_client']) to determine whether this is the first ever run of the pipeline. is_first_run_of_pipeline() does just as the function of the same name that is a first lambda handler utility function does. 

6) If this is the first ever run of the pipeline calls create_dim_date_Parquet()
to convert the table into a Parquet file in a BytesIO buffer. 
calls make_dim_date_python(), makes a date dimension table, which 
create_dim_date_Parquet() returns.

7) calls upload_to_s3() to save the date dimension table (which is in the form of Parquet data in a BytesIO buffer) to the processed bucket.



8) If this is the 2nd-plus run of the pipeline the second lambda handler calls make_dim_or_fact_table(), which returns a dimension table or the fact table. 

If the table name is ‘staff’ or ‘counterparty’ make_dim_or_fact_table() calls make_staff_or_cp_dim_table(), which returns either the staff dimension table or the counterparty dimension table. 

To create the staff dimension table this function must get data from the latest department table in the ingestion bucket. It retrieves that table and calls func_lookup_table(table_name). which returns function transform_to_dim_staff()
make_staff_or_cp_dim_table()calls transform_to_dim_staff(), which makes and returns the staff dimension table. It calls make_dictionary(), a helper function. 


To create the counterparty dimension table this function must get data from the latest address table in the ingestion bucket. It retrieves that table and calls func_lookup_table(table_name). which returns function transform_to_dim_counterparty()
make_staff_or_cp_dim_table()calls transform_to_dim_staff(), which makes and returns the staff dimension table. It calls make_dictionary(), a helper function. 


	  
 <br><br><br>



# Running Tests

To run the tests, execute the following in the command line after having navigated to the project directory:  <br>

```bash
	pytest -vvvrP
```
 <br>
