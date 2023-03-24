from pymongo import MongoClient

user = 'root'
passw = 'toor'
client = MongoClient(f"mongodb://{user}:{passw}@localhost:27017/")

# Connect to our database and collection
db = client['FlightsBase']
collection = db['flights']
insert_list = [
    {"name":"Tom","surname":"Riddle", "from":"London", "to":"NewYork","date":"2022-05-18T16:00:00Z"},
    {"fullname":"Harry Smith", "from":"Moscow", "to":"Saint-Petersburg","date":"2018-06-20T18:30:00Z"},
    {"name":"Mary","surname":"Adamson", "from":"Paris", "to":"Berlin","date":"2016-12-19T10:50:00Z","price":"400"},
    {"fullname":"Natasha Romanoff", "from":"Moscow", "to":"Budapest","date":"2023-03-24T16:55:00Z","price":1000},
    {"fullname":"Alex Ruben", "from":"Venice", "to":"Glasgow","date":"2000.03.15","age":30},
    {"name":"Tony","surname":"Stark", "from":"Dublin", "to":"Budapest","date":"2023-03-24", "age":"40"},
]

collection.insert_many(insert_list)
print(f'{list(db["flights"].find())=}')