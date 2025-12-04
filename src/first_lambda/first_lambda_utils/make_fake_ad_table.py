import random

from datetime import datetime, date, time, timedelta



def make_fake_ad_table():
    """
    This function:
        makes a fake address
        table in the form of a 
        list of dictionaries.

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the 
        address table from the 
        totesys database.        
    
    """

    base_day = datetime(2025, 11, 13, 13, 17, 19) 
    td_1_day = timedelta(days=1) 

    add_table = [
    {
    'address_id': None, 
    'address_line_1': '6826 Herzog Via', 
    'address_line_2': None, 
    'district': 'Avon', 
    'city': 'New Patienceburgh', 
    'postal_code': 28441, 
    'country': 'Turkey', 
    'phone': '1803 637401', 
    'created_at': None, 
    'last_updated': None
    }, 

    {
    'address_id': None, 
    'address_line_1': '179 Alexie Cliffs', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Aliso Viejo', 
    'postal_code': '99305-7380', 
    'country': 'San Marino', 
    'phone': '9621 880720', 
    'created_at': None, 
    'last_updated': None
    },
    
    {
    'address_id' : None, 
    'address_line_1': '148 Sincere Fort', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Lake Charles', 
    'postal_code': 89360, 
    'country': 'Samoa', 
    'phone': '0730 783349', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '6102 Rogahn Skyway', 
    'address_line_2': None, 
    'district': 'Bedfordshire', 
    'city': 'Olsonside', 
    'postal_code': 47518, 
    'country': 'Republic of Korea', 
    'phone': '1239 706295', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '34177 Upton Track', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Fort Shadburgh', 
    'postal_code': '55993-8850', 
    'country': 'Bosnia and Herzegovina', 
    'phone': '0081 009772', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '846 Kailey Island', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Kendraburgh', 
    'postal_code': '08841', 
    'country': 'Zimbabwe', 
    'phone': '0447 798320', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None,
    'address_line_1': '75653 Ernestine Ways', 
    'address_line_2': None, 
    'district': 'Buckinghamshire', 
    'city': 'North Deshaun', 
    'postal_code': '02813', 
    'country': 'Faroe Islands', 
    'phone': '1373 796260', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '0579 Durgan Common', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Suffolk', 
    'postal_code': '56693-0660', 
    'country': 'United Kingdom', 
    'phone': '8935 157571', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '644 Edward Garden', 
    'address_line_2': None, 
    'district': 'Borders', 
    'city': 'New Tyra', 
    'postal_code': '30825-5672', 
    'country': 'Australia', 
    'phone': '0768 748652', 
    'created_at': None, 
    'last_updated': None
    },

    {
    'address_id': None, 
    'address_line_1': '49967 Kaylah Flat', 
    'address_line_2': 'Tremaine Circles', 
    'district': 'Bedfordshire', 
    'city': 'Beaulahcester', 
    'postal_code': 89470, 
    'country': "Democratic People's Republic of Korea", 
    'phone': '4949 998070', 
    'created_at': None, 
    'last_updated': None
    }
    ]

    for i in range(1, 11):  # 1, 2, 3 ... 10
        add_table[i-1]['address_id'] = i
        add_table[i-1]['created_at'] = base_day
        add_table[i-1]['last_updated'] = base_day
        for ky, vl in add_table[i-1].items():
            if add_table[i-1][ky] == None:
                add_table[i-1][ky] = '' # remove None 


    return add_table


