





# typical "counterparty" table

# "counterparty" table cols and typical values# 
# [['counterparty_id'], ['counterparty_legal_name'], ['legal_address_id'], 
# ['commercial_contact'], ['delivery_contact'], ['created_at'], ['last_updated']]
# [[20, 'Yost, Watsica and Mann', 2, 'Sophie Konopelski', 'Janie Doyle', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]

# get rid of:
# commercial_contact
# delivery_contact
# created_at 
# last_updated



# dim_counterparty  {
# Already have:
#   counterparty_id int [pk, not null]
#   counterparty_legal_name varchar [not null]

# Need to create:
#   counterparty_legal_address_line_1 varchar [not null]
#   counterparty_legal_address_line_2 varchar
#   counterparty_legal_district varchar
#   counterparty_legal_city varchar [not null]
#   counterparty_legal_postal_code varchar [not null]
#   counterparty_legal_country varchar [not null]
#   counterparty_legal_phone_number varchar [not null]
# }
