import psycopg2
from pymongo import MongoClient
from datetime import datetime

# Create the client
user = 'root'
passw = 'toor'
client = MongoClient(f"mongodb://{user}:{passw}@localhost:27017/")

# Connect to our database
db = client['FlightsBase']

# Connect to postgres
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="root",#user="root",
    password="123123",#password="123123",
    port="5432")
cursor = conn.cursor()
list_flights = []
i=1
deleted = 0 

f = open("metadata.txt", "w")
f.write("Import data from MongoDB to PostgreSQL into the flights table\n")
f.write(f"Current time: {datetime.now()}\n")

for row in db["flights"].find():
    one_dict = {'passport' : '', 'from':'','to':'','date_from':'','date_to':'','price':0}
    
    if "passport" in row:
        # passp = str(row['passport'])[:4]+" "+str(row['passport'])[4:]
        one_dict.update({'passport' : row['passport']})
    elif "series" in row:
        # full = str(row["series"])+" "+str(row["number"])
        full = row["series"]*1000000+row["number"]
        one_dict.update({'passport' : full})
    
    if (row['from']==row['to']):
        print("Same town: ",i)
        deleted+=1
    else:
        one_dict.update({'from' : row['from']})
        one_dict.update({'to' : row['to']})
    
    format='%Y-%m-%d %H:%M:%S.%f'
    date_from = datetime.strptime(row['date_from'],format)
    date_to = datetime.strptime(row['date_to'],format)
    if (date_from < date_to):
        one_dict.update({'date_from' : row['date_from']})
        one_dict.update({'date_to' : row['date_to']})
    else:
        one_dict.update({'date_to' : row['date_from']})
        one_dict.update({'date_from' : row['date_to']})
        print("Time incorrect: ",i, date_from, date_to)

    if "price" in row:
        one_dict.update({'price' : int(row['price'])})
    
    if (row['from']!=row['to']):
        # list_flights.append(one_dict)
        cursor.execute(f"INSERT INTO flights (passport,from_town, to_town, date_from, date_to, price) VALUES ({one_dict['passport']},'{one_dict['from']}','{one_dict['to']}',TO_TIMESTAMP('{one_dict['date_from']}','YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('{one_dict['date_to']}','YYYY-MM-DD HH24:MI:SS'),{one_dict['price']});")
    i+=1

#print(list_flights)
f.write(f"Number of entries in Mongo: {i}\n")
f.write(f"Number of entries deleted: {deleted}\n")
f.write(f"Number of entries inserted into Postgres: {i-deleted}\n")

# print(cursor.execute("select * from flights;"))
# for i,val in enumerate(list_flights):
#     # print(val)
#     cursor.execute(f"INSERT INTO flights (passport,from_town, to_town, date_from, date_to, price) VALUES ({val['passport']},'{val['from']}','{val['to']}',TO_TIMESTAMP('{val['date_from']}','YYYY-MM-DD HH24:MI:SS'),TO_TIMESTAMP('{val['date_to']}','YYYY-MM-DD HH24:MI:SS'),{val['price']});")
conn.commit()
conn.close()
f.close()