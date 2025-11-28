
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

The first lambda function <br><br>
**1)** calls function `get_env_vars()`, which creates a lookup table of values that the first lambda function requires, such as a pg8000.Native Connection object, which the first lambda function needs to connect to the totesys OLTP database.

**2)** calls function `change_after_time_timestamp()`. <br>
`change_after_time_timestamp()` tries to read the timestamp string stored in the ingestion bucket. <br>
If it succeeds it returns that string. <br>
If it fails it returns the default timestamps string, which represents the year 1900 ("1900-01-01T00-00-00"). <br>


**3)** calls function `get_data_from_db()`. <br>
`get_data_from_db()` employs a loop in which it calls function `read_table()` on every table name.

**4)** In the loop, `read_table()` does this for each table: <br>

At this point the first lambda function behaves differently depending on the boolean value of environment variable IS_OLTP_OK. <br>
Database totesys stopped containing data of interest in November 2025, so the behaviour of function `read_table()` has to take this into account. <br>

After November 2025 IS_OLTP_OK is set to False. This makes function `read_table()` create fake tables. <br>
Before November 2025 IS_OLTP_OK is set to True. This makes function `read_table()` read real tables from OLTP database totesys. <br>
 <br>
If IS_OLAP_OK is True:  <br>

<br>

**i)** function `read_table()` calls function `get_column_names()`. 
<br>

`get_column_names()` makes an SQL query to database totesys to get the column names of the table in question. <br>
This function returns a lists of lists, each member list containing one string, which is a column name. 
<br>

**ii)** calls function `get_updated_rows()`. 
<br>

`get_updated_rows()` makes an SQL query to the totesys database to get those rows of the table in question that have been updated after the time indicated in the timestamp returned by function `change_after_time_timestamp()`. <br> 
<br>
This function returns a lists of lists, each member list representing a row. In the first ever run of the pipeline, this function retrieves all of the rows of a table. 
<br>	
<br>

**iii)** Changes the columns list of lists to a list of strings, like this: <br>
	['name', 'location', etc] 
<br>

**iv)** calls function `convert_values()`.
	`convert_values()` changes cell value types like this: 
<br>
	 datetime.datetime object -> ISO string
     Decimal value            -> float
     json                     -> string
<br>

**v)** calls function `make_row_dicts()` to make a list of row data like this: <br>

`	[ 
   {"design_id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00.123456', etc},         
   {"design_id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-06-01T10:30:00.123456', etc},
    #   etc 
	]
`	

<br>

**vi)** makes the list of rows the value of a dictionary key that is the name of the table, like this: <br>
<br> 
 `{ "sales_order”: # ‘sales_order’ is name of the table
	[  
{"Name": "xx", "Month": "January", “sales”_id”: 3, etc}, <-a row
{"Name": "yy", "Month": "January", "sales”_id": 4, etc}, <-a row
{"Name": "zz", "Month": "January", "sales”_id": 9, etc}, <-a row
    etc
     ] 
 }`
 <br>

**vii)** returns the dictionary above.

<br>

Function read_table() then makes the list of dictionaries the value of a key in a dictionary. The key is the name of the table in question.
<br>
In the same loop function `get_data_from_db()` appends the dict returned by read_table() to a list.
<br>
On the first ever run of the first lambda function the list will contain every table and every row of each table. 
<br>
On subsequent runs of the first lambda function the list will more likely contain dictionaries for less than all of the tables (only the ones that have had rows updated) and each list in such a dictionary will contain only the updated rows of the table (most likely not all of the rows):
<br>
Function `get_data_from_db()` then returns this dict:
`	[ 
  {sales_orders: [{...}, {...}, etc}]},
  {design: 	  [{...}, {...}, etc}]},
	etc
	]
`
<br>

If IS_OLAP_OK is False:  <br>

Function `read_table()` calls function `make_fake_xx_table()`,  where xx is one of so, de, ad, st, cu, cp, dp. Function `make_fake_xx_table()` makes a fake version of the table in question. For example `make_fake_cp_table()` makes a fake counterparty table. <br>

Function 'read_table()' then returns the table in a dictionary, for example: <br>
{'design': -*-list of design table rows here-*-}  <br>


Still in the loop, function `get_data_from_db()` calls function `make_data_json_safe()`, which converts data in the table into a form that allows its conversion to json.  <br>

Function `get_data_from_db()` then appends the table to a list and returns the list, which looks like this: <br>
`[{'design': [{<updated-row data>}, etc]}, {'sales_order': [{<updated-row data>}, etc]}, etc]` <br>

The first lambda handler then:  <br>

4) Calls function `write_to_s3()`, passing in the list of dictionaries returned by `get_data_from_db()`.
<br>

`write_to_s3()` 
<br>



`write_to_s3()` then loops through the dictionaries passed in and for each determines whether one or more tables of the same name already exist in the ingestion bucket and: 
<br>
	i)  if no such table exists already that means this is the first ever run of the pipeline, so `write_to_s3()` jsonifies the list in each dictionary (ie this list:  <br>
<br>	

` [  
{"sales_order_id": 1, "Name": "xx xx", "Month": "January", etc},  #a row
{"sales_order_id": 2, "Name": "yy yy", "Month": "January", etc},  #a row 
{"sales_order_id": 3, "Name": "zz zz", "Month": "January",  etc}, #a row 
    etc  
 ] `
	 
for example)	 <br>
<br>	 
	and saves it in the ingestion bucket under the key timestamp/table-name.json <br>

ii)  if such a table already exists in the ingestion bucket that means this is the 2nd-plus run of the pipeline, so the list in the dictionary contains only     updated rows of the table in question. <br>

So `write_to_s3()` calls function `write_to_ingestion_bucket()`. 	<br>

`write_to_ingestion_bucket()` reads from the ingestion bucket the most recent table of the given name, updates the appropriate rows in latest_table and saves the table as a new table in the ingestion bucket under the key timestamp/table-name.json. The pre-existing table remains in the ingestion bucket. <br>

The table stored under that key looks like a jsonified version of this: <br>

`		       [  
{"Name": "xx", "Month": "January", “sales”_id”: 1, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 2, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 3, etc},#a row 
    etc
     	   ]
`

 <br>	     

ie it’s just the table data, not the name of the table.   <br>
The name of the table is in the key (along with the timestamp).   <br>
The table contains all rows, some of which will have been updated.   <br>
The previous table remains in the ingestion bucket.	 <br>

When the first lambda puts a table in the ingestion bucket, AWS S3 sends an event to the second lambda, which triggers it.
 




### Operation of the second lambda function <br>
<br>

The second lambda handler <br>
1) runs in response to the event it received from AWS S3 when the first lambda has put a table into the ingestion bucket.   <br>
2) calls second_lambda_init() to create a lookup table in the form of a dictionary. Functions that the second lambda handler calls employ values they obtain from the lookup table.  <br> 
3) receives info about which table the first lambda just saved in the ingestion bucket in parameter events   <br>
4) calls function `read_from_s3()` to retrieve that table from the ingestion bucket. The second lambda handler then converts the data from json format to a python list that looks like this:   <br>

`		       [  
{"Name": "xx", "Month": "January", “sales”_id”: 1, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 2, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 3, etc},#a row 
    etc
     	   ]
`
<br>

5) calls function `should_make_dim_date()`, which determines whether this is the first ever run of the pipeline. If it is, `should_make_dim_date()` creates a date dimension table and puts it in the processed bucket. <br>
`should_make_dim_date()` calls  <br>
&nbsp;&nbsp;&nbsp;&nbsp; function `is_first_run_of_pipeline()`, which determines whether the processed bucket is empty (which would mean that it is the first ever run of the pipeline). This function returns True if it is the first ever run of the pipleline, in which case  <br>
&nbsp;&nbsp;&nbsp;&nbsp; should_make_dim_date() also calls  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; function `create_dim_date_Parquet()`, which calls:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;  &nbsp;&nbsp;&nbsp;&nbsp; function `make_dim_date_python()` to make the date dimension table <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;  &nbsp;&nbsp;&nbsp;&nbsp; calls function convert_to_parquet(), which converts the date dimension table to Parquet form, puts it in a BytesIO buffer and returns the buffer.  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;  &nbsp;&nbsp;&nbsp;&nbsp; calls function `make_column_defs()`, which makes a string of column names,  <br>
	
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;  &nbsp;&nbsp;&nbsp;&nbsp; calls function `make_parts_of_insert_statements()`,  which makes parts of insert statements that this function will use to create a DuckDB table from the Python list table.  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;  &nbsp;&nbsp;&nbsp;&nbsp; calls function `put_pq_table_in_temp_file()`, which creates a DuckDB database, creates the Parquet file in it and calls function `write_parquet_to_buffer()`, which writes the Parquet file to a BytesIO buffer and returns the buffer, which convert_to_parquet() returns.  <br>

&nbsp;&nbsp;&nbsp;&nbsp; `create_dim_date_Parquet()` then creates a key for the Parquet-in-buffer and returns it and the key as members of a list.
&nbsp;&nbsp;&nbsp;&nbsp; `should_make_dim_date()` then calls function `upload_to_s3()` to save the Parquet-in-buffer to the processed bucket.  <br>

6) calls function `make_dim_or_fact_table()`, which creates the fact table if the table name is 'sales_order' and makes a dimension table if the table name is anything else.  <br>

If the table name is 'staff', making a staff dimension table needs special attention because that table needs information from the 'department' table, so `make_dim_or_fact_table()` calls function `make_staff_or_cp_dim_table()`, which gets the 'department' table from the ingestion bucket, takes the required information from it. creates the 'staff' dimension table and returns it.  <br>

Similarly if the table name is 'counterparty', the function gets the address table, retrieves information from it and makes the counterparty dimension table and returns it.  <br>

The table takes the usual form: [{<row-data>}, {<row-data>}, {<row-data>}, etc], without the table name.  <br>

7) calls `convert_to_parquet()` directly to convert the dimension/fact table to Parquet form inside an IOBytes buffer.
	convert_to_parquet(data) (where data is a list representing either a 
	dimension table or the fact table) follows this procedure:
	1) takes values out of the table represented by python list data
	2) makes insert statement strings from those values. the insert statements are 	necessary to put the data into a table in duckdb and dave the table in duckdb 
	in Parquet format.
	3) saves the duckdb Parquet table in and IObytes buffer.
	To achieve 1) to 3) convert_to_parquet(data) calls these functions:
	make_column_defs(data), which makes a string of column names
	make_insert_statements(data), which is badly names and actually 
	makes parts of the insert statements
	convert_to_parquet(data) then makes temporary file path tmp_path
	put_pq_table_in_temp_file(), which makes the Parquet table and 
	puts it in the temporary file path.
	write_parquet_to_buffer(tmp_path), which makes a BytesIO()
	buffer, opens the Parquet file in tmp_path and saves the Parquet file into the
	buffer, then deletes tmp_path and returns the buffer
	convert_to_parquet(data) then returns the buffer
	
8) makes the appropriate key for the table, either:  <br>
	i) fact_sales_order/<timestamp-here>.parquet  <br>
		or  <br>
	ii) dim_design/<timestamp-here>/.parquet   <br>
9) Calls function `upload_to_s3()` to save the table (now in Parquet form in a buffer) to the processed bucket under the appropriate key  <br>
10) Putting the table in the processed bucket makes S3 trigger the third lambda function.  <br>
 <br>



Operation of the third lambda function  <br>
--------------------------------------  <br>
The third lambda handler
1) receives information about which table the second lambda handler just saved in the ingestion bucket in parameter events and calls xxxx()  <br>



3) calls function `make_insert_queries_from_parquet()`, which makes a list of INSERT SQL query strings to direct at the data warehouse to insert row data into table in question in the warehouse.  <br>

`make_insert_queries_from_parquet()` calls <br>

`read_parquet_from_buffer()`, which returns [conn, columns, column_str, rows],  <br>
where conn is a Duckdb in-memory database  <br>
columns is a list of column names, eg  <br>
`				['xx', 'yyy', 'zzz', 'abcdef'] 
				column_str is a comma-seprated string of col names, eg
				'xx, yyy, zzz, abcdef'] 
`  <br>
rows is a list of tuples, each tuple containing row data, eg  <br>
`				[ 
					(1,  'xxx',   75.50)  	,
		               (2,  'yyy',   82.00),
				     (3,  'zzz',   69.75),
	        			  etc
			     ]
`   <br>		

`make_insert_queries_from_parquet()` then calls <br>
`make_list_of_query_strings()`, which generates a list of SQL INSERT statements. <br>
`make_list_of_query_strings()`, employs a loop inside which it calls:  <br>
		
`make_list_of_formatted_row_values()`, which employs a loop inside which is a call to  <br>
`format_value(value)`, which formats the values of each row so that they can be put inside an SQL INSERT string.
			
4) calls `make_SQL_queries_to_warehouse()`, which reads the list of SQL queries and makes the queries to the warehouse, thus putting the row data in the appropriate table in the warehouse. Each table in the warehouse may have several versions of a particular row, each version showing data that was current when the row was written to the table in the warehouse.
	  
 <br><br><br>



# Running Tests

To run the tests, execute the following in the command line after having navigated to the project directory:  <br>

```bash
	pytest -vvvrP
```
 <br>
