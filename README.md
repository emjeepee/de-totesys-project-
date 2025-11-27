
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
 - Three AWS Lambda functions labelled first lambda, second lambda and third lambda
 - Two AWS S3 buckets labelled the ingestion bucket and the processed bucket
 - AWS EventBridge notifications
 - AWS CloudWatch logging for each lambda function
 <br><br><br>


## Operation of the project:
### **Overview** <br>
This project converts data from OLTP database totesys into denormalised data in the form of dimensions tables and a fact table, which this project writes into an AWS RDS postgresql data warehouse.  <br>

**1)** An AWS EventBridge scheduler triggers the first lambda function every five minutes.  <br> <br> 
**2)** The first lambda function behaves differently depending no whether the pipeline is running <br>
<br>
	**a)** for the first time ever 
<br> <br>
	**b)** for the 2nd-plus time 
<br>

**i)** In the case of a) the first lambda function makes SQL queries to read all rows of all tables in online transaction processing (OLTP) database toteSys.  <br> 
**ii)** In the case of b) the first lambda function instead makes the queries to read only the updated rows of tables.   <br> 
**iii)** for either case a) or b) the first lambda function converts each partial-table or whole-table data into a partial/whole table in the form of a Python list of dictionaries, where each dictionary represents a row and its key-value pairs are column-name: field-value pairs   <br> 
**iv)** converts each whole/partial Python table into json form and puts it in the ingestion bucket   <br>   

**3)** Writing a table to the ingestion bucket makes the AWS S3 service trigger an event to which the second lambda responds.   <br>

**4)** The second lambda:   <br> 
	i) runs in response to the trigger from S3    <br> 
	ii) gets the just-saved table from the ingestion bucket, transforms it into a dimension or fact table in the form of a list of dictionaries (where each dictionary represents a row and its key-value pairs are column-name: field-value pairs) and converts the table into a Parquet file in a BytesIO buffer   <br> 
	iii) puts the buffer containing the Parquet dimension/fact table in the processed bucket under a key of this form: "timestamp:table-name"   
<br> 

**5)** Writing a table to the processed bucket makes the AWS S3 service trigger an event to which the third lambda responds.   <br> 

**6)**  The third lambda:   <br> 
	i) runs in response to the trigger from S3
<br> <br> 
	ii) gets from the processed bucket the just-saved buffer containing the Parquet file   
<br> 
	iii) converts the Parquet file into a Duckdb table in memory   
<br> 
	iv) employs DuckDB to read the table in the Parquet file into an in-memory DuckDB database   
<br> 
	v) employs DuckDB to create SQL query strings from each row of the table   
<br> 
	vi) uses the query strings to insert each row into the appropriate table in the online analytical processing (OLAP) data warehouse.   <br> 
 <br>

<br><br><br>

### **In detail** <br>
Operation of the first lambda function   <br> <br>
--------------------------------------
The first lambda function 
**1)** calls function `get_env_vars()`, which creates a lookup table of values that the first lambda function requires, such as a pg8000.Native Connection object, which the first lambda function needs to connect to the totesys OLTP database.

**2)** calls function `change_after_time_timestamp()`. <br>
`change_after_time_timestamp()` tries to read the timestamp string stored in the ingestion bucket. <br>
If it succeeds it returns that string. <br>
If it fails it returns the default timestamps string, which represents the year 1900 ("1900-01-01T00-00-00"). <br>


**3)** calls function `get_data_from_db()`. <br>
`get_data_from_db()` employs a loop in which it calls function `read_table()` on every table name.

**4)** In the loop, `read_table()` does this for each table: <br>
<br>
**i)** calls function `get_column_names()`. 
<br>
	`get_column_names()` makes an SQL query to database totesys to get the column names of the table in question. <br>
	This function returns a lists of lists, each member list containing one string, which is a column name. 
<br>

**ii)** calls function `get_updated_rows()`. 
<br>	
	`get_updated_rows()` makes an SQL query to the totesys database to get the rows of the table in question that have been updated after the time indicated in the timestamp returned by function `change_after_time_timestamp()`. <br> 
<br>
	This function returns a lists of lists, each member list representing a row. In the first ever run of the pipeline, this function   
	retrieves all of the rows of a table. 
<br>	
<br>

**iii)** Changes the columns list of lists to a list of strings, like this:
	['name', 'location', etc] 
<br>

**iv)** calls function `convert_values()`.
	`convert_values()` changes cell value types like this: 
<br>
	 datetime.datetime object -> ISO string
     Decimal value            -> float
     json                     -> string
<br>

**v)** calls make_row_dicts(clean_col_names, cleaned_rows) to make a list of row data like this: <br>
`    	 [ 
   {"design_id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00.123456', etc},         
   {"design_id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-06-01T10:30:00.123456', etc},
    #   etc ]
`	
<br>

**vi)** puts the list of rows into a dictionary as the value of a key that is the name of the table like this: <br>
<br> 
 `{ "sales_order”: # ‘sales_order’ is name of the table
	[  
{"Name": "xx", "Month": "January", “sales”_id”: 3, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 4, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 9, etc},#a row 
    etc
     ] 
 }`
 <br>

**vii)** returns the dictionary above.

<br>

In the same loop get_data_from_db() appends the dict returned by 
read_table() (ie the dict in code 1 above) to a list.
On the first ever run of the first lambda function the list will contain 
every table and every row of each table. 
On subsequent runs of the first lambda function the list will more likely contain 
dictionaries for less than all of the tables (only the ones that have had rows updated) 
and each list in such a dictionary will contain only the updated rows of the table (most likely not all of the rows):
<br>
get_data_from_db() returns this dict:
`	[ 
  {sales_orders: [{...}, {...}, etc}]},
  {design: 	  [{...}, {...}, etc}]},
	etc
	]
`
<br>

4) Calls function `write_to_s3()`.
<br>

`write_to_s3()` first creates a timestamp by calling `timestamp = create_formatted_timestamp()`
<br>
`write_to_s3()` then loops through the dicts in data_for_s3 and for each determines whether one or more tables of the same name already exist in the ingestion bucket and: 
<br>
	i)  if no such table exists already that means this is the first ever run of the pipeline, so write_to_s3() jsonifies the list in each dictionary (ie this list:  
`	[  
{"Name": "xx", "Month": "January", “sales”_id”: 1, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 2, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 3, etc},#a row 
    etc  
     ]`     for example)	 <br>
	    and saves it in the ingestion bucket under the key timestamp/table-name.json <br>
	ii)  if such a table already exists in the ingestion bucket that means this is the 2nd-plus run of the pipeline, so the list in the dictionary contains only     updated rows of the table in question. <br>
	So `write_to_s3()` calls function `write_to_ingestion_bucket()` <br>
`write_to_ingestion_bucket()` reads from the ingestion bucket the most recent table of the given name, like this:
	    `latest_table = get_most_recent_table_data(file_location, s3_client, bucket)` <br>
	   
`write_to_ingestion_bucket()` then updates the appropriate rows in latest_table and saves the table as a new table in the ingestion bucket under the key timestamp/table-name.json. The pre-existing table remains in the ingestion bucket. <br>

The table stored under that key looks like this but jsonified:
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

4) When the first lambda puts a new table (with updated rows) in the ingestion bucket, AWS S3 triggers the second lambda.
 




Operation of the second lambda function
---------------------------------------
second_lambda_handler(event, context) 
1) runs in response to an event sent by AWS S3 to the second lambda when the first lambda has put a table into the ingestion bucket (in the form of a .json file).
2) calls second_lambda_init() to create a lookup table in the form of a dictionary.
3) receives info about which table the first lambda just saved in the ingestion bucket in param events 
4) calls read_from_s3() to retrieve that table from the ingestion bucket. The second lambda then converts the data to a python list that looks like this:
		       [  
{"Name": "xx", "Month": "January", “sales”_id”: 1, etc},#a row
{"Name": "yy", "Month": "January", "sales”_id": 2, etc},#a row 
{"Name": "zz", "Month": "January", "sales”_id": 3, etc},#a row 
    etc
     	   ]
5) calls should_make_dim_date(), which creates a date dimension table if this is the first ever run of the pipeline and stores it in the processed bucket.
should_make_dim_date() calls
	is_first_run_of_pipeline(), which determines whether the processed bucket is empty (meaning that it is the first ever run of the pipeline). This function returns True if it is the first ever run of the pipleline, in which case
should_make_dim_date() also calls 
	create_dim_date_Parquet(), which runs this:
	dim_date_py = make_dim_date_python(start_date, num_rows) 
	where dim_date_py is a python list of dictionaries 

	create_dim_date_Parquet() then calls 
	dim_date_pq = convert_to_parquet(dim_date_py)
	where dim_date_pq is the Parquet version of the dimension date table
	inside a BytesIO buffer.
	convert_to_parquet() calls make_column_defs(), which makes a 
	str of column names, 
	then calls make_parts_of_insert_statements(), 
	which makes parts of insert statements that this function will use to create a 
	Duckdb table from the Python list table.
	then calls put_pq_table_in_temp_file(), which makes a DuckDb 		database and makes the Parquet file in it
	then calls write_parquet_to_buffer(), which writes the parque file to a 
	BytesIO buffer and returns the buffer, which convert_to_parquet() 
	returns.

	create_dim_date_Parquet() then creates a key for the dim_date_pq 
	table and returns [dim_date_pq, dim_date_key].
should_make_dim_date() then calls upload_to_s3() to save the 
	dim_date_pq Parquet-in-buffer to the processed bucket. 

6) calls make_dim_or_fact_table(), which makes the fact table if the table name is ‘sales_order’ and makes a dimension table if the table name is anything else. 
If the table name is ‘staff’, making a ‘staff’ dimensions table needs special attention because that table needs information from the ‘department’ table, so make_dim_or_fact_table() calls make_staff_or_cp_dim_table(), which 
gets the ‘department’ table, takes the required information from it and makes the ‘staff’ dimension table and returns it.
Similarly if the table name is ‘counterparty’, the function gets the address table, retrieves information from it and makes the counterparty dimension table and returns it. 
The table takes the usual form: [{<row-data>}, {<row-data>}, {<row-data>}, etc], without the table name. 
7) calls convert_to_parquet() directly to convert the dim/fact table to Parquet form inside an IOBytes buffer.
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
	
8) makes the appropriate key for the table, either:
	i) fact_sales_order/<timestamp-here>.parquet
		or
	ii) dim_design/<timestamp-here>/.parquet  
9) Calls upload_to_s3() to save the table (now in Parquet form in a buffer) to the processed bucket under the appropriate key
10) Putting the table in the processed bucket makes S3 trigger the third lambda function.

Operation of the third lambda function
--------------------------------------------------
third_lambda_handler(event, context)
1) receives info about which table the first lambda just saved in the ingestion bucket in param events and 
2) calls xxxx()



3) calls make_insert_queries_from_parquet(parquet_buffer, table_name), which makes a list of INSERT SQL query strings to direct at the data warehouse to insert rows data into table in question in the warehouse. 
This function first calls 
		read_parquet_from_buffer(parquet_buffer),  TESTED 
			which returns [conn, columns, column_str, rows]
			where conn is a Duckdb in-memory database
				columns is a list of column names, eg
				['xx', 'yyy', 'zzz', 'abcdef'] 
				column_str is a comma-seprated string of col names, eg
				'xx, yyy, zzz, abcdef'] 
				rows is a list of tuples, each tuple containing row data, eg
				[ 
					(1,  'xxx',   75.50)  	,
		               (2,  'yyy',   82.00),
				     (3,  'zzz',   69.75),
	        			  etc
			     ]
		
Then this function calls
	make_list_of_query_strings(rows, table_name, column_str), 	which generates a list of SQL INSERT statements. 
	make_list_of_query_strings(), employs a for loop inside which it 
	calls: IN THE PROCESS OF TESTING THURS13NOV25
		make_list_of_formatted_row_values(row), which employs a 		for loop and inside that calls: TESTED
			format_value(value),  TESTED which format the values of 
			each row so that they can be put inside an SQL INSERT string.
			
4) calls make_SQL_queries_to_warehouse(), which takes that list of SQL queries and makes them to the warehouse, thus putting the row data in the appropriate table in the warehouse. Each table in the warehouse may have several versions of a particular row, each version showing data that was current when the row was written to the table in the warehouse.
	  
 <br><br><br>



# Running Tests

To run the tests, execute the following in the command line after having navigated to the project directory:

```bash
	pytest -vvvrP
```
