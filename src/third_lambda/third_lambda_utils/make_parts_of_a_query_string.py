



def make_parts_of_a_query_string(cols, vals):
    """
    This function:
        1) makes parts of a query 
            string that other utility 
            functions of the third 
            lambda will employ to 
            insert or update data in 
            tables in the warehouse.
        2) gets called by these two 
            functions:             
            i) make_query_for_one_row_fact_table()
            and 
            ii) make_query_for_one_row_dim_table().

    Args:
        1) cols: a list of column names, eg
        ['design_id', 'xxx', 'yyy'] 
        2) vals: a list of values for one row,
        eg [13, 'NULL', 'cabbage']                     
    
    """


    # cols is ['design_id', 'xxx', 'yyy']
    cols_str = ", ".join(cols) 

    # vals is [13, 'NULL', 'turnip']
    vals_str = ", ".join(
        str(v) if isinstance(v, int) or v in ("NULL", "TRUE", "FALSE") else f"'{v}'"
        for v in vals
                        ) # the arg of join() is 
                          # a generator expression


    # get rid of ', ' and add 
    # round brackets: 
    cols_str = '(' + cols_str + ')' #  '(design_id, xxx, yyy)'
    vals_str = '(' + vals_str + ')' # "(13, NULL, 'cabbage')"

    return [cols_str, vals_str]








    # cols_str = vals_str = ''
    # for i in range(len(cols)):                      # cols is ['www', 'xxx', 'yyy', 'zzz'], vals is [13, '1', 'NULL', 'turnip']
    #     cols_str += f'{cols[i]}, '                  # after loop cols_str is '(design_id, xxx, yyy, zzz, )'
    #     if type(vals[i]) is int: 
    #         vals_str += f"{str(vals[i])}, "            
    #     if type(vals[i]) is str: 
    #         if vals[i] == 'NULL' or vals[i] == 'TRUE' or vals[i] == 'FALSE': # get rid of inverted commas:
    #             vals_str += f"{vals[i]}, "
    #         else: # eg if 'turnip' keep the inverted commas:
    #             vals_str += f"'{vals[i]}', "
