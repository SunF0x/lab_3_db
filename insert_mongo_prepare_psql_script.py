from pymongo import MongoClient
import json, ast
import random
from datetime import datetime, timedelta

city_list = ['London','Paris','Moscow','Saint-Petersburg','Dubai','Tokyo','Singapore','Barselona','Madrid','Rome',
             'Doha','Abu Dhabi','San Francisco','Amsterdam','Toronto','Sydney','Berlin','New York', 'Los Angeles', 'Chicago', 
             'Houston', 'Philadelphia','Praga','Washington','Istanbul','Las Vegas','Seoul','San Diego','Miami','Milan',
             'Vein','Dublin','Vancouver','Boston','Melbourne','Houston','Seattle','Montreal','Hong Kong','Frankfurt',
             'Tel Aviv','Copenhagen','Atlanta','Dallas','Lisbon','Oslo','Denver','Delhi','Brussels','Portland']

def gen_datetime(min_year=2000, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()

def generator():
    with open('generate100.txt','w') as f:
        for i in range(1,101):
            time_from = gen_datetime()
            if (i%3==0):
                f.write("{"+f"\"passport\":{random.randrange(1000000000, 9999999999)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from+timedelta(hours=random.randrange(1,17))}\",\"price\":{random.randrange(11455, 1300102)}"+"}")
            elif (i%5==1):
                f.write("{"+f"\"series\":{random.randrange(1000, 9999)},\"number\":{random.randrange(000000, 999999)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from+timedelta(hours=random.randrange(1,17))}\",\"price\":{random.randrange(11455, 1300102)}"+"}")
            elif (i%7==0):
                f.write("{"+f"\"passport\":{random.randrange(1000000000, 9999999999)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from-timedelta(hours=random.randrange(1,17))}\",\"price\":{random.randrange(11455, 1300102)}"+"}")
            else:
                f.write("{"+f"\"passport\":{random.randrange(1000000000, 9999999999)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from+timedelta(hours=random.randrange(1,17))}\",\"price\":\"{random.randrange(11455, 1300102)}\""+"}")
            f.write("\n") 

# generate new_file
generator()

user = 'root'
passw = 'toor'
client = MongoClient(f"mongodb://{user}:{passw}@localhost:27017/")

# Connect to our database and collection
db = client['FlightsBase']
collection = db['flights']
insert_list = []

with open("generate100.txt") as file:
    for row in file:
        #print(row.strip())
        insert_list.append(ast.literal_eval(str(row.strip()))) 

#print(insert_list)
collection.insert_many(insert_list)
print(f'{list(db["flights"].find())=}')

import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="root",#user="root",
    password="123123",#password="123123",
    port="5432")
cursor = conn.cursor()
cursor.execute("""
                 CREATE TABLE flights (id serial PRIMARY KEY, passport bigint NOT NULL, from_town varchar, to_town varchar, date_from TIMESTAMP, date_to TIMESTAMP, price bigint);
                 """)
conn.commit()
conn.close()