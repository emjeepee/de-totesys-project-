## **Extract-transform-load (ETL) pipeline** 


## Description
This project creates an extract-transform-load (ETL) data pipeline. <br> <br>
The pipeline <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
- reads table data from a postgresql online transaction processing (OLTP) <br> 
database  
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
- transforms the data so that it is compatible with the star-schema model   <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
- writes the data to a postgresql online analytical processing (OLAP) data  <br> 
warehouse. 

 <br><br><br>


## Project directories
The most important directories in this project are:
 - **.github** -- contains the GitHub Actions workflow.yml file <br><br>
 - **src** -- contains these directories: <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
first_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
directory first_lambda_utils -- which contains utility functions that the first  <br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
python module first_lambda_handler.py, which contains the code for the first <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
lambda handler.  <br>  <br>


&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
second_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
directory second_lambda_utils -- which contains utility functions that the   <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
second lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
python module second_lambda_handler.py, which contains the code for the second <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
lambda handler.  <br>  <br>

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
third_lambda, which contains:  <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
directory third_lambda_utils -- which contains utility functions that the third  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
lambda handler will employ <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
python module third_lambda_handler.py, which contains the code for the third <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
lambda handler. <br><br>

 - **terraform** -- this directory contains all the Terraform *.tf files that <br>
 together provision the cloud services that this project employs. <br> 
 This project employs three AWS Lambda functions, two S3 buckets,  <br> 
 AWS CloudWatch, an AWS EventBridge scheduler and AWS EventBridge notifications.  <br> <br> 
 See the separate README.md file in this directory for details about how this <br> 
 project employs infrastructure as code.<br><br>

 - **tests** -- contains test_*.py files. These function employ testing library <br> 
 pytest to test all of the python code that makes up the lambda handlers and <br> 
 their utility functions.
 <br><br><br>



## Author

- Mukund Pandit (email: mukund.panditman@googlemail.com)<br>
<br><br><br>



## Installation

Setup instructions:  <br>
 - fork this GitHub repository: https://github.com/emjeepee/de-totesys-project- <br> <br>
 - in GitHub Actions trigger file workflow.yml (which is in directory .github) <br>
either manually in GitHub Actions or via a push to the remote repository.

 <br><br><br>
    
## Tech stack
 - Cloud services in AWS:  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 S3  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 Lambda  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 EventBridge  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 CloudWatch
 
 <br> <br>

 - Code: <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 Python 3.12 <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 Python libraries:  <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *botocore*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *calendar*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *copy*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *currency-codes*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *datetime*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *decimal*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *dotenv*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *duckdb* <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *io*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *json*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *logging*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *os*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *pg8000.native*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *tempfile*  <br> <br>

 - CI/CD: <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 GitHub Actions <br> <br>

 
 - Testing: <br> 
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 Python libraries:   <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *coverage*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *moto*  <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *Pytest*   <br>
 &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
 *unittest* <br> <br>

 
 <br><br><br>



## Cloud infrastructure that this project provisions
 - An AWS EventBridge scheduler  <br>  <br> 
 - Three AWS Lambda functions labelled first lambda, second lambda and third <br> 
 lambda  <br>  <br> 
 - Two AWS S3 buckets labelled the ingestion bucket and the processed bucket <br><br> 
 - An S3 bucket that contains the code for the lambda functions <br> <br> 
 - An S3 bucket for the terraform state 
 - AWS EventBridge notifications <br><br> 
 - AWS CloudWatch logging for each lambda function  <br> <br> 

For more details see separate README.MD in directory terraform.	

 <br><br><br><br><br>


## Operation of the pipeline – overview:
This project converts data from postgresql OLTP database totesys into  <br> 
denormalised data in the form of dimension tables and a fact table, which this  <br> 
project writes into an AWS RDS postgresql data warehouse.  <br>

To carry this out the following occurs: <br>

**1)** An AWS EventBridge scheduler triggers the first lambda function every  <br> 
five minutes.  <br> <br> 
**2)** The first lambda function behaves differently depending on whether the  <br> 
pipeline is running <br>
<br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 	
**a)** for the first time ever 
<br> <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 	
**b)** for the 2nd-plus time 
<br>

**i)**  <br> 
If a) is true the first lambda function makes SQL queries to read all rows of  <br> 
all tables in OLTP database totesys.  <br>   <br> 
**ii)**  <br> 
If b) is true the first lambda function instead makes the queries to read only  <br> 
the updated or new rows of tables in database totesys.   <br>   <br> 
**iii)**  <br> 
In either case a) or b) the first lambda function converts each partial-table  <br> 
or whole-table data into a Python list of dictionaries, where each dictionary  <br> 
represents a row and its key-value pairs are columnname-fieldvalue pairs. <br><br> 
**iv)**  <br> 
If a) is true the first lambda function converts each whole table into json  <br> 
form and puts it in the ingestion bucket.   <br>   <br> 
**v)**  <br> 
If b) is true the first lambda function reads the ingestion bucket for the  <br> 
latest version of a table, replaces its rows with the rows that contain updated  <br> data, converts the table into json form and puts it in the ingestion bucket as  <br> 
a new table, leaving the original latest table unchanged.   <br>     

**3)** The AWS S3 service responds to the writing of a table to the ingestion  <br> 
bucket by sending an event to the second lambda function, causing the second  <br> 
lambda function to run.   <br>

**4)** The second lambda:   <br> 
i) runs in response to the trigger from S3  <br>  <br> 
ii) gets the just-saved table from the ingestion bucket, transforms it into a  <br> 
dimension or fact table in the form of a list of dictionaries (where each  <br> 
dictionary represents a row and its key-value pairs are column-name:  <br> 
field-value pairs)    <br>  <br> 
iii) converts the table into a Parquet file and puts it into a BytesIO buffer   <br>  <br> 
iv) puts the buffer containing the Parquet dimension table or fact table in the <br> 
 processed bucket under a key of this form: "timestamp:table-name"    <br> 
<br> 

**5)** Writing a table to the processed bucket makes the AWS S3 service trigger  <br> 
an event to which the third lambda responds.   <br> 

**6)**  The third lambda:   <br> 
i) runs in response to the trigger from S3
<br> <br> 
ii) gets the just-saved buffer (that contains a table in Parquet format) from  <br> 
the processed bucket    
<br> 
iii) converts the Parquet file into a DuckDB table in memory   
<br> 
iv) employs DuckDB to read the table in the Parquet file into an in-memory  <br> DuckDB database   
<br> 
v) employs DuckDB to create SQL query strings from each row of the table   
<br> 
vi) uses the query strings to insert each row into the appropriate table in the  <br> 
OLAP data warehouse.   <br> 
 <br>

<br><br><br>







## Operation of the pipeline – detailed:
<br>

### Operation of the first lambda function   <br> 
The first lambda function:    <br> 

1) Checks the value of environment variable WHAT_ENV. <br>
In development the value is "dev".  <br>
Code employs library dotenv to load all locally set environment variables from <br>
file .env.  <br>
In production the value is "prod", set by terraform code. <br>
Terraform code sets the environments variables (for AWS Lambda).<br>
<br>

2) calls get_env_vars(), which returns a lookup table that contains various <br> 
values the first lambda handler requires, for example a boto3 S3 client object <br>
and the names of the ingestion and processed buckets. <br>
<br>

3) calls change_after_time_timestamp(), which tries to read the timestamp <br> 
string stored in the ingestion bucket.  <br>
If it succeeds it returns that string.  <br>
If it fails it returns default timestamp string "1900-01-01T00-00-00", which <br> 
represents the year 1900.  <br>
  <br>

4) calls get_data_from_db(), which employs a loop in which it calls <br>
read_table() on every table name.  <br>
read_table()behaves differently depending on the value of environment  <br>
variable IS_OLTP_OK.  <br> <br> 
If IS_OLTP_OK is True this means database totesys contains useful data  <br>
(pre-Nov2025) <br> 
In the loop, read_table() does this for each table:  <br>
i) calls get_column_names(), which makes an SQL query to the database totesys 
<br>to get the column names of the table in question. This function returns a <br>
list of lists, each member list containing one member, a string for a column <br>
name <br>
ii) calls get_updated_rows(), which makes an SQL query to database totesys to <br>
get the rows of the table in question that have been updated after time <br>
after_time. <br>
This function returns a lists of lists, each member list representing a row and <br>
containing field values. In the first ever run of the pipeline, this function   <br>
retrieves all of the rows of a table. <br>
iii) changes the columns list of lists to a list of strings that looks like 
this: 	[‘xxx’_id, 'location', etc] <br>
iv) calls convert_values() to change cell value types like this:<br>
datetime.datetime object -> ISO string <br>
Decimal value            -> float <br>
json                     -> string <br>
v) calls make_row_dicts() to make a list of rows that looks like this:<br>
```    	 
	[ 
   {"design_id": 6,  "name": "aaa", etc},         
   {"design_id": 7,  "name": "bbb", etc},
       etc 
	]
```  
<br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
and to return the list of rows in a dictionary as the value of a key that is <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
the name of the table like this: <br>

```
 { "sales_order”: # name of the table
	[  
{"Name": "xx", "Month": "January", “sales”_id”: 3, etc}, # a row
{"Name": "yy", "Month": "January", "sales”_id": 4, etc}, # a row 
{"Name": "zz", "Month": "January", "sales”_id": 9, etc}, # a row 
    etc
     ] 
 } 
 ``` 

<br>
<br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
vi) returns the dict above  <br>
<br><br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
If IS_OLTP_OK is False:<br> 
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
database totesys no longer contains useful data (post-Nov2025) and this <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
pipeline employs dummy data. <br>

&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
So, in the loop, read_table() creates a fake tables and returns dictionary  <br>
&nbsp;&nbsp;&nbsp;&nbsp; &nbsp;&nbsp;&nbsp;&nbsp; 
```
{table_name: row_list_of_dicts}  
```
<br>



5) calls reorder_list(), which puts the address table and department table <br> 
at indexes [0] and [1] of the list that get_data_from_db() returned.  <br> 

6) calls is_first_run_of_pipeline(). That function lists the objects in the  <br>
processed bucket. If there are objects in the bucket tha means this is not the  <br>
first ever run of the pipeline. This function returns false.  <br> 
If there are no objects there then this is the first ever run of the pipline. <br>
This function returns True.  <br> 

7) If is_first_run is True the first lambda handler calls  <br> 
write_tables_to_ing_buck() to save the table in the ingestion bucket in json <br> 
format.  <br> 
This function calls create_formatted_timestamp() to make a timestamp, then  <br> 
calls save_updated_table_to_S3() to put the table in the ingestion bucket in  <br> 
json format under a key that includes the timestamp. <br>
If is_first_run is False the first lambda handler calls make_updated_tables(), <br>
which calls get_most_recent_table_data(), which calls get_latest_table(). <br> 
For each table in the list of updated tables that reorder_list() returns, <br> 
make_updated_tables() gets the latest table of the same name from the ingestion <br> 
bucket and updates its rows with the rows of that table in the list of updated  <br> 
tables. make_updated_data() then returns a dictionary that looks like this:<br> 
```
          [
          {'design': [{row}, {row}, {row}, etc]},
          {'sales_order': [{row}, {row}, {row}, etc]},
          etc
          ]
```
		 
 <br> 		 
The list that is the value of the key ‘design’, for example, contains all of <br> 
those rows of the design table that now contain updated or new rows.  <br> 

The first lambda handler then calls  write_tables_to_ing_buck() to save each  <br> 
table in the list above to the ingestion bucket under a new key and as a  <br> 
jsonified dictionary. In this way the first lambda handler puts an updated  <br> 
table in the ingestion bucket while leaving the previous version of the table  <br> 
there.  <br> 
 
8) calls lookup['close_db'](lookup['conn’]), which closes the connection to  <br> 
database totesys. <br>  <br> 

When the first lambda handler puts a new table (with updated rows) in the  <br> 
ingestion bucket, AWS S3 triggers the second lambda. <br> <br> 


### Operation of the second lambda function   <br> 

The second lambda handler <br> 
1) runs when AWS S3 sends an event to it after the first lambda handler has put  <br> 
a table into the ingestion bucket. <br> 

2) calls second_lambda_init() to create a lookup table in the form of a  <br> 
dictionary from which it can access values it requires.<br> 

3) determines whether the table just put into the ingestion bucket is the  <br> 
department table. If so the second lambda handler stops running because it has  <br> 
no need to create a department dimension table. <br> 

4) if the table just put into the ingestion bucket is NOT the department table <br> 
the second lambda handler calls read_from_s3() to retrieve that table.<br> 
The second lambda handler converts the table into a list. <br> 

5) calls is_first_run_of_pipeline() to determine whether this is the first <br> 
ever run of the pipeline. 

6) If is_first_run_of_pipeline() returns True the second lambda handler calls <br> 
create_dim_date_Parquet() to make a date dimension table, convert it into a  <br> 
Parquet file in a BytesIO buffer and return the buffer. <br> 
create_dim_date_Parquet() first calls make_dim_date_python() to make the Python  <br> 
list version of the date dimension table, then calls convert_to_parquet() to 
convert the date dimension table in Python list form into a Parquet file in a <br> 
BytesIO buffer.

7) Calls upload_to_s3() to write the date dimension table now in Parquet form <br> 
in a BytesIO buffer to the processed bucket.  <br> 

8) If this is the 2nd-plus run of the pipeline the second lambda handler calls  <br> 
make_dim_or_fact_table(), which returns a dimension table or the fact table in    <br> 
the form of a Python list.

If the table name is ‘staff’ or ‘counterparty’ make_dim_or_fact_table() calls  <br> 
make_staff_or_cp_dim_table(), which returns either the staff dimension table or  <br> 
the counterparty dimension table.   <br> 

To create the staff dimension table make_staff_or_cp_dim_table() must get data 
from the latest department table in the ingestion bucket. It retrieves that  <br> 
table and calls func_lookup_table(table_name), which returns function  <br> 
transform_to_dim_staff().  <br> 
make_staff_or_cp_dim_table() then runs transform_to_dim_staff(), which makes  <br> 
and returns the staff dimension table. <br> 

To create the counterparty dimension table this function must get data from the  <br> 
latest address table in the ingestion bucket. It retrieves that table and calls  <br> 
func_lookup_table(table_name), which returns function  <br> 
transform_to_dim_counterparty().   <br> 
make_staff_or_cp_dim_table() runs transform_to_dim_staff(), which makes and  <br> 
returns the staff dimension table.<br> <br> 

### Operation of the third lambda function   <br>

The third lambda handler runs after receiving an event from AWS S3 when the <br> 
second lambda handler puts a table in Parquet form in a BytesIO buffer in the 
processed bucket.

The third lambda handler 

1) calls third_lambda_init() to create a lookup table that contains values the <br>
code requires.

2) calls get_inbuffer_parquet(), which gets from the processed bucket the <br>
BytesIO buffer that contains the table (which is in Parquet form) that the  <br>
second lambda handler has just put in that bucket. 

3) calls make_insert_queries_from_parq(), which makes a list of SQL queries <br>
from the table.

4) calls make_SQL_queries_to_warehouse(), which contacts the warehouse and  <br>
inserts table rows into the appropriate table there. <br>

5) Calls close_db() to close the connection to the warehouse.
 <br> <br><br> <br>


# Running Tests

To run the tests, execute the following in the command line after having  <br> 
navigated to the project directory:  <br>

```bash
	pytest -vvvrP
```
 <br>
