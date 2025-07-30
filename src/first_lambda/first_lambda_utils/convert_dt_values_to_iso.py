from src.first_lambda.first_lambda_utils.serialise_datetime import serialise_datetime


def convert_dt_values_to_iso(list_of_rows):
    """
    This function:
        Changes any datetime objects in
        the member lists of list_of_rows
        to iso format.

    Args:
        list_of_rows: a list of lists. Each 
        member list of list_of_rows represents 
        a row of a table (and contains just the 
        cell values, not the column names).

    Returns:
        A version of list_of_rows where each member
        list now contains iso format time strings
        where datatime objects previously existed. 
    """
    list_to_return = [  [ serialise_datetime(list_of_rows[i][j])         for j in range(len(list_of_rows[0])) ]   for i in range(len(list_of_rows))      ]

    
    return list_to_return

