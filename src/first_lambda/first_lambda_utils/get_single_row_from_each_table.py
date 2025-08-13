import os

from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


def get_single_row_from_each_table():

    conn = conn_to_db('TOTE_SYS')

    tables = [
        "design",
        "payment",
        "sales_order",
        "transaction",
        "sales_order",
        "counterparty",
        "address",
        "staff",
        "purchase_order",
        "department",
        "currency",
        "payment_type",
              ]

    list_to_print = []

    for i in range(len(tables)):

        query_row_data = f'SELECT * FROM {tables[i]} ORDER BY {tables[i]}_id DESC LIMIT 1;'
        # get row data:
        try:
            row_data = conn.run(query_row_data)
            list_to_print.append(row_data)
        except Exception as e:
            print(f"Error in trying to get row data: {e}")

        # get col names:
        query_col_names = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tables[i]}' ORDER BY ordinal_position"
        try:
            column_names = conn.run(query_col_names) 
            # column_names = [col[0] for col in column_names]
            list_to_print.append(column_names)            
        except Exception as e:
            print(f"Error in trying to get column names: {e}")



        script_dir = os.path.dirname(__file__)  # directory of this Python file
        file_path = os.path.join(script_dir, "test_file.txt")

    # # Write the list to file test_file.txt:
    with open(file_path, "w") as f:
        for item in list_to_print:
            f.write(str(item) + "\n")  # each item on a new line
        


    # with open(file_path, "w") as f:
    #     f.write("this is a test")

    
get_single_row_from_each_table()    



