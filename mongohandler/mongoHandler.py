import ast
import random
from datetime import datetime, timedelta
from pymongo import MongoClient


def migrate(client: MongoClient, row_count, mongo_meta):
    generator(row_count)

    # Connect to our database and collection
    client.drop_database(mongo_meta['database'])
    db = client[mongo_meta['database']]
    collection = db[mongo_meta['collection']]
    insert_list = []

    with open("generate.txt") as file:
        for row in file:
            insert_list.append(ast.literal_eval(str(row.strip())))

    collection.insert_many(insert_list)


def gen_datetime(min_year=2000, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def generator(row_count, start_passport=1000000000):
    city_list = ['London', 'Paris', 'Moscow', 'Saint-Petersburg', 'Dubai', 'Tokyo', 'Singapore', 'Barselona',
                 'Madrid',
                 'Rome',
                 'Doha', 'Abu Dhabi', 'San Francisco', 'Amsterdam', 'Toronto', 'Sydney', 'Berlin', 'New York',
                 'Los Angeles', 'Chicago',
                 'Houston', 'Philadelphia', 'Praga', 'Washington', 'Istanbul', 'Las Vegas', 'Seoul', 'San Diego',
                 'Miami', 'Milan',
                 'Vein', 'Dublin', 'Vancouver', 'Boston', 'Melbourne', 'Houston', 'Seattle', 'Montreal',
                 'Hong Kong',
                 'Frankfurt',
                 'Tel Aviv', 'Copenhagen', 'Atlanta', 'Dallas', 'Lisbon', 'Oslo', 'Denver', 'Delhi', 'Brussels',
                 'Portland']

    with open('generate.txt', 'w') as f:
        for i in range(1, 10001):
            time_from = gen_datetime()
            if i % 3 == 0:
                f.write(
                    "{" + f"\"passport\":{random.randrange(start_passport, start_passport + row_count)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from + timedelta(hours=random.randrange(1, 17))}\",\"price\":{random.randrange(11455, 1300102)}" + "}")
            elif i % 5 == 1:
                f.write(
                    "{" + f"\"series\":1000,\"number\":{random.randrange(0, row_count)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from + timedelta(hours=random.randrange(1, 17))}\",\"price\":{random.randrange(11455, 1300102)}" + "}")
            elif i % 7 == 0:
                f.write(
                    "{" + f"\"passport\":{random.randrange(start_passport, start_passport + row_count)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from - timedelta(hours=random.randrange(1, 17))}\",\"price\":{random.randrange(11455, 1300102)}" + "}")
            else:
                f.write(
                    "{" + f"\"passport\":{random.randrange(start_passport, start_passport + row_count)},\"from\":\"{random.choice(city_list)}\",\"to\":\"{random.choice(city_list)}\",\"date_from\":\"{time_from}\",\"date_to\":\"{time_from + timedelta(hours=random.randrange(1, 17))}\",\"price\":\"{random.randrange(11455, 1300102)}\"" + "}")
            f.write("\n")

            # generate new_file


def mongo_read(client, mongo_meta):
    logfile = open("mongo_logfile", "w")
    res = []
    db = client[mongo_meta['database']]
    for row in db[mongo_meta['collection']].find():
        one_dict = {'passport': '', 'from': '', 'to': '', 'date_from': '', 'date_to': '', 'price': 0}

        if "passport" in row:
            one_dict.update({'passport': row['passport']})
        elif "series" in row:
            # full = str(row["series"])+" "+str(row["number"])
            full = row["series"] * 1000000 + row["number"]
            logfile.write(f"Series modified: {full}\n")
            one_dict.update({'passport': full})

        one_dict.update({'from': row['from']})
        one_dict.update({'to': row['to']})

        date_format = '%Y-%m-%d %H:%M:%S.%f'
        date_from = datetime.strptime(row['date_from'], date_format)
        date_to = datetime.strptime(row['date_to'], date_format)
        if date_from < date_to:
            one_dict.update({'date_from': row['date_from']})
            one_dict.update({'date_to': row['date_to']})
        else:
            one_dict.update({'date_to': row['date_from']})
            one_dict.update({'date_from': row['date_to']})
            logfile.write(f"Date to/from modified: {one_dict['date_from']} {one_dict['date_to']}\n")

        if "price" in row:
            one_dict.update({'price': int(row['price'])})
            if type(row["price"]) is str:
                logfile.write(f"Price convert from string: {row['price']}\n")
        res.append(one_dict.values())

    return res
