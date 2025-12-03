



formatted_values = ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"', '"O''Mally"']
table_name = 'design'
column_str = 'xxx'
value_list = ', '.join(formatted_values) # '5, "xyx", 75.5, "TRUE", "NULL", "O''Mally"'
query_str = f'INSERT INTO {table_name} ({column_str}) VALUES ({value_list});' 

# query_str must end up like this:
# 'INSERT INTO design (aaa, bbb, ccc) VALUES (5, "xyz", "TRUE", "NULL");'


var_1 = '5'
var_2 = "5"