from enum import Enum

from benedict import benedict

class Dilects(Enum):
    TSQL = 1
    MYSQL = 2

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


db_connections = benedict({
    'Remote' :'remote',
    'Remote1' :'remote',
    'Local' :'local',
})

db_dilects = benedict({
    'Remote' : Dilects.TSQL.name,
    'Remote1' :  Dilects.TSQL.name,
    'Local' : Dilects.TSQL.name,
})

db_names = benedict({
    'Remote' : 'HeavyDev',
    'Remote1' :  'HeavyDev',
    'Local' : 'HeavyDev',
})

schema_names = benedict({
    'Remote' : 'dbo',
    'Remote1' :  'dbo',
    'Local' : 'dbo',
})