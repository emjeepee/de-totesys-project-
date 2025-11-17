from .transform_to_star_schema_fact_table import transform_to_star_schema_fact_table
from .transform_to_dim_staff import transform_to_dim_staff
from .transform_to_dim_design import transform_to_dim_design
from .transform_to_dim_counterparty import transform_to_dim_counterparty
from .transform_to_dim_currency import transform_to_dim_currency
from .transform_to_dim_location import transform_to_dim_location





# This dictionary acts as a look up table for functions.
# Each key of the dictionary is the name of a table
# in the ToteSys database and the first half of the key 
# under which the ingestion bucket stores tables.
# For example if a key for a table in the ingestion bucket
# is 'design/20255-06-13_13:13:13' then the corresponding
# key in the dictionary below is 'design'

# The value of each key is a function that creates 
# either the fact table (in the case of key 
# 'sales_order') or a dimension table.


def func_lookup_table(table_name: str):

    lookup_table = {
    "sales_order": transform_to_star_schema_fact_table, # tested
    "staff": transform_to_dim_staff,
    "address": transform_to_dim_location, # converts address table to location dim table
    "design": transform_to_dim_design,
    "counterparty": transform_to_dim_counterparty,
    "currency": transform_to_dim_currency,
                   }
    
    fn_to_return = lookup_table[table_name]

    return fn_to_return