import copy




def reorder_list(lst, *items):
    """
    This function:
        finds certain items in a
        list and moves them to the 
        front of the list.

    Args:
        lst: the list that contains 
        the items that need to be 
        reordered. It looks like this:
         [
       {'design': [{}, {}, {}]},
       {'sales_order': [{}, {}, {}]},
       {'counterparty': [{}, {}, {}]},
       {'department': [{}, {}, {}]},
       {'staff': [{}, {}, {}]},
       {'address': [{}, {}, {}]},
                    ]

        *items: the items that need
        to be moved to the front of 
        the list. These are the 
        names of keys, eg 'department'
        and 'address'. This function 
        will move those dicts with 
        those single keys in the 
        order that they appear in 
        *items to the front of the 
        list. *items takes this form:
        "address", "department"

    Returns:
        a new list with the specified
        items at the front of the 
        list. 

    
    """

    list_to_return = []
    lst_deep_copy = copy.deepcopy(lst)
    for item in items: # items is, eg, ('department', 'address')
        for member in lst: # member is, eg, {'design': [{}, {}, {}] }
            if item in member: # eg if 'department' is in {'department': [{}, {}, {}] }
                member_deep_copy = copy.deepcopy(member)
                lst_deep_copy.remove(member)
                list_to_return.insert(0, member_deep_copy)

    list_to_return.extend(lst_deep_copy)

    return list_to_return
