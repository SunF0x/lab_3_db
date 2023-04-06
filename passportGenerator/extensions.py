import random
import time
import json
from datetime import datetime

from faker import Faker

# Faker.seed(10)
fake = Faker("ru_RU")


def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%d.%m.%Y', prop)


ru_en_dict_file = "../namesDictionary.json"
ru_en_FileJson = json.load(open(ru_en_dict_file))
dict_len_names = len(ru_en_FileJson["en_names"])
dict_len_lastnames = len(ru_en_FileJson["en_lastnames"])
dict_len_sex_abbr = len(ru_en_FileJson["en_sex_abbr"])
dict_len_country = len(ru_en_FileJson["en_country_abbr"])


def translateCountryFromEnToRu(country):
    return translateFromTo("en_country_abbr", "ru_country_abbr", country)


def randomFakeCountry():
    return ru_en_FileJson["en_country_abbr"][random.choice(range(dict_len_country))]


def checkIfValueInList(en_name, value):
    return value in ru_en_FileJson[en_name]


def translateGenderFromEnToRu(gender):
    return translateFromTo("en_sex_abbr", "ru_sex_abbr", gender)


def randomFakeGender():
    return ru_en_FileJson["en_sex_abbr"][random.choice(range(dict_len_sex_abbr))]


def translateNameFromEnToRu(en_name):
    return translateFromTo("en_names", "ru_names", en_name)


def randomFakeEnName():
    return ru_en_FileJson["en_names"][random.choice(range(dict_len_names))]


def translateLastNameFromEnToRu(en_name):
    return translateFromTo("en_lastnames", "ru_lastnames", en_name)


def randomFakeEnLastName():
    return ru_en_FileJson["en_lastnames"][random.choice(range(dict_len_lastnames))]


def translateFromTo(src_field, dst_field, value):
    value_index = ...
    for index in range(len(ru_en_FileJson[src_field])):
        if ru_en_FileJson[src_field][index] == value:
            value_index = index
            break

    if value_index is ...:
        return None

    return ru_en_FileJson[dst_field][value_index]
