from enum import Enum


clm_name_mapping = {
    'name': 'Name',
    'email': 'Email',
    'address': 'Address',
    'date_of_birth': 'Date of Birth',
    'phone_number': 'Phone Number',
    'username' : 'User Name',
    'start_time' : 'Start Time',
    'end_time' : 'End Time',
    'description' : 'Description',
    'category' : 'Category',
    'location' : 'Location',
    'product' : 'Product',
    'id' : 'ID'
}

direction = {
    '0': 'asc','1': 'desc', 0: 'asc', 1: 'desc'
}

rev_direction = {
    0:1, 1:0, '0':1, '1':0
}


class Dilects(Enum):
    TSQL = 1
    MYSQL = 2