import random

from datetime import datetime, date, time, timedelta



def make_fake_st_table():
    """
    This function:
        makes a fake staff
        table in the form of a 
        list of dictionaries.

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the 
        staff table from the 
        totesys database.        
    
    """

    base_day = datetime(2025, 11, 13) 
    td_1_day = timedelta(days=1) 

    staff_table = [
        {'staff_id': None, 
         'first_name': 'Jeremie', 
         'last_name': 'Franey', 
         'department_id': 4, 
         'email_address': 'jeremie.franey@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Deron', 
         'last_name': 'Beier', 
         'department_id': 1, 
         'email_address': 'deron.beier@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Jeanette', 
         'last_name': 'Erdman', 
         'department_id': 2, 
         'email_address': 'jeanette.erdman@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Ana', 
         'last_name': 'Glover', 
         'department_id': 4, 
         'email_address': 'ana.glover@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Magdalena', 
         'last_name': 'Zieme', 
         'department_id': 3, 
         'email_address': 'magdalena.zieme@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Korey', 
         'last_name': 'Kreiger', 
         'department_id': 3, 
         'email_address': 'korey.kreiger@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Raphael', 
         'last_name': 'Rippin', 
         'department_id': 2, 'email_address': 
         'raphael.rippin@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Oswaldo', 
         'last_name': 'Bergstrom', 
         'department_id': 4, 
         'email_address': 'oswaldo.bergstrom@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Brody', 
         'last_name': 'Ratke', 
         'department_id': 2, 
         'email_address': 'brody.ratke@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None}, 

        {'staff_id': None, 
         'first_name': 'Jazmyn', 
         'last_name': 'Kuhn', 
         'department_id': 3, 
         'email_address': 'jazmyn.kuhn@terrifictotes.com', 
         'created_at': None, 
         'last_updated': None} 
                ]

    for i in range(1, 11):  # 1->10 incl
        staff_table[i-1]['staff_id'] = i
        staff_table[i-1]['created_at'] = base_day
        staff_table[i-1]['last_updated'] = base_day


    return staff_table