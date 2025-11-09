

def make_insert_statements(data):

    # Get the first row from the 
    # table so that code can 
    # later determine the column
    # names:
    columns = list(data[0].keys())

    placeholders = ', '.join('?' for _ in columns)
    values_list = []

    for row in data:
        values = [str(row[col]) for col in columns]  # or keep raw types if needed
        values_list.append(values)

    return placeholders, values_list

