import random

from datetime import datetime, date, time, timedelta



def make_fake_cp_table():
    """
    This function:
        makes a fake counterparty
        table in the form of a 
        list of dictionaries.

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the 
        counterparty table from 
        the totesys database.        
    
    """

    base_day = datetime.datetime(2025, 11, 13) 
    td_1_day = timedelta(days=1) 



    return curr_table