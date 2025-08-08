from src.second_lambda.second_lambda_utils.transform_to_star_schema_fact_table import transform_to_star_schema_fact_table
from src.second_lambda.second_lambda_utils.transform_to_dim_staff import transform_to_dim_staff
from src.second_lambda.second_lambda_utils.transform_to_dim_design import transform_to_dim_design
from src.second_lambda.second_lambda_utils.transform_to_dim_counterparty import transform_to_dim_counterparty
from src.second_lambda.second_lambda_utils.transform_to_dim_currency import transform_to_dim_currency
from src.second_lambda.second_lambda_utils.transform_to_dim_location import transform_to_dim_location





# This dictionary acts as a look up table for functions.
# The keys of of the dictionary are the names of 
# tables. A key is also the first half of the key under 
# which the ingestion bucket stores tables.
# For example if a key for a table in the ingestion bucket
# is 'design/20255-06-13_13:13:13' then the corresponding
# key in the dictionary below is 'design'

# The values of each key is a functions that creates 
# either the facts table (in the case of key 'sales_order')
# or a dimension table.


function_lookup_table = {
"sales_order": transform_to_star_schema_fact_table,
"staff": transform_to_dim_staff,
"address": transform_to_dim_location, # address table converted to location dim table
"design": transform_to_dim_design,
"counterparty": transform_to_dim_counterparty,
"currency": transform_to_dim_currency,
                        }