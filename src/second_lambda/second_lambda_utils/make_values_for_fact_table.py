import datetime





def make_values_for_fact_table(ca, lu, apd, add, up):
    """
    This function:
        receives as arguments 
        values from the 
        sales_orders table that 
        an earlier function 
        read from the 
        ingestion bucket and 
        converts them into forms
        that are more easily 
        converted into types 
        required for inclusion 
        in SQL insert queries. 

    Args:
        ca:         

    """


    dt_created = ca
    dt_created_time = dt_created.time() # extract time only
    dt_created_date = dt_created.date() # extract date only

    dt_updated = lu
    dt_updated_time = dt_updated.time() # extract time only
    dt_updated_date = dt_updated.date() # extract date only

    apd_str = apd # eg '2025-08-16'
    dt_apd = datetime.strptime(apd_str, "%Y-%m-%d").date()

    add_str = add # eg '2025-09-20'
    dt_add = datetime.strptime(add_str, "%Y-%m-%d").date()

    up_dec = up
    up_form = f"{up_dec:.2f}" # eg '3.56'