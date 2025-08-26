





def make_parts_of_qstr_dim_tables(cols: list, vals: list):
    """
    This function:
        1) makes parts of query strings
            that code will employ to 
            update or insert data into a
            table in the warehouse. 
        1) creates three strings:
            i)   a string of columns all 
                 in round brackets, eg
                 '(design_id, xxx, yyy, zzz)'
            ii)  a string of row values
                 all in round brackets,
                 eg '(13,  '1',  NULL, 'turnip')'
            iii) a string of column-value 
                 pairs with an equals 
                 sign between each column
                 and its value, eg
                 "design_id = 13, xxx = '1', yyy = NULL, zzz = 'turnip'"

    Args:
        1) cols: a list of strings, each 
            string being the name of a column,
            eg ['design_id', 'xxx', 'yyy', 'zzz']
        2) vals: a list of strings, each 
            string being the name of a row,
            value, eg [13, '1', 'NULL', 'turnip']

    Returns:
        A list that contains:
            i)   the string of column names 
                  in round brackets, eg
                  '(design_id, xxx, yyy, zzz)'            
            ii)  a string of row values all
                 in round brackets, eg
                 '(13,  '1',  NULL, 'turnip')'
            iii)  a string of column-value 
                 pairs with an equals 
                 sign between each column
                 and its value, eg
                 "design_id = 13, xxx = '1', yyy = NULL, zzz = 'turnip'"
                 
                 
    """

    cols_str = vals_str = col_val_pairs = ''
    for i in range(len(cols)):                      # cols is ['www', 'xxx', 'yyy', 'zzz'], vals is [13, '1', 'NULL', 'turnip']
        cols_str += f'{cols[i]}, '                  # after loop cols_str is '(design_id, xxx, yyy, zzz, )'
        if type(vals[i]) is int: 
            vals_str += f"{str(vals[i])}, "            
        if type(vals[i]) is str: 
            if vals[i] == 'NULL': # get rid of inverted commas:
                vals_str += f"{vals[i]}, "
            else: # eg if '1' or 'turnip' keep the inverted commas:
                vals_str += f"'{vals[i]}', "

    # get rid of ', ' and add 
    # round brackets: 
    cols_str = '(' + cols_str[:-2] + ')'
    vals_str = '(' + vals_str[:-2] + ')'

                                                    # after loop vals_str is 
                                                    # cols_str is 'xxx, yyy, zzz'
                                                    # vals_str is '1, NULL, turnip'  
                                                    # col_val_pairs is 'xxx = 1, yyy = NULL, zzz = turnip;'

    cols_str = '(' + cols_str[:-2] + ')'            # '(design_id, xxx, yyy, zzz)'
    
    vals_str = '(' + vals_str + ')'                 # '(1, NULL, turnip)' 
    col_val_pairs = col_val_pairs + ';'             # 'xxx = 1, yyy = NULL, zzz = turnip;'


    for i in range(len(cols_str)):
        col_val_pairs += f'{cols[i]} = {vals[i]}, ' 
    col_val_pairs = col_val_pairs + ';'          


    return [cols_str, vals_str, col_val_pairs]        
    