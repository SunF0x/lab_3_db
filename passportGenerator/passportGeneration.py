from passportGenerator import extensions
import random
from enum import Enum


passport_issue_age = 14

genders = ["жен", "муж"]

ruinRecordMaxIndex = 1000
ruinRecordMinIndex = ruinRecordMaxIndex - 6

"""Generate data for passport"""
"""     id - XXXXYYYYYY"""
"""     last name, first name - String?, String?"""
"""     Birthday - date (error - month > 12 || month < 1, day > 31, year < 1900)"""
"""     Sex - String (male, female, error - no / "")"""
"""     Registration address - (Fake )"""
"""     Passport issue date - (error - month > 12 || month < 1, day > 31, year < 1900, date - birthday < 14 - error )"""


def randomlyAddPunctuations(name_part):
    if random.random() > 0.5:
        name_part += "&"
    return name_part


def errorFirstName():
    return randomlyAddPunctuations(extensions.randomFakeEnName())


def errorLastName():
    return randomlyAddPunctuations(extensions.randomFakeEnLastName())


def errorInSex():  # ml - will replace with male, fml - with female, others - drop
    return extensions.randomFakeGender()


def errorCountry():
    return extensions.randomFakeCountry()


def errorInBirthdayDate(random_date):
    str_date = str(random_date).split('.')
    str_date[2] = str(random.randrange(50, 90))
    return ".".join(str_date)


def errorInIssuanceDate(birthday):
    new_value = str(birthday).split('.')
    new_value[2] = str(int(new_value[2]) + random.choice(range(passport_issue_age)))
    return '.'.join(new_value)


"""     id - XXXXYYYYYY"""
"""     last name, first name - String?, String?"""
"""     Birthday - date (error - month > 12 || month < 1, day > 31, year < 1900)"""
"""     gender - String (male, female, error - no / "")"""
"""     Registration address - (Fake )"""
"""     Passport issue date - (error - month > 12 || month < 1, day > 31, year < 1900, date - birthday < 14 - error )"""

passportId = 1000000000


class Fields(Enum):
    FIRSTNAME = 1
    LASTNAME = 2
    GENDER = 3
    ADDRESS = 4
    BIRTH = 5
    PASS_ISSUE = 6


# ruinRecordMaxIndex-1 - error in last name
# ruinRecordMaxIndex-2 - error in first name
# ruinRecordMaxIndex-3 - error in sex
# ruinRecordMaxIndex-4 - error in address (fake)
# ruinRecordMaxIndex-5 - error in date
# ruinRecordMaxIndex-6 - error in passport issuance date

def generatePassportData(count):
    dict_error = {
        str(ruinRecordMaxIndex - 1): errorFirstName,
        str(ruinRecordMaxIndex - 2): errorLastName,
        str(ruinRecordMaxIndex - 3): errorInSex,
        str(ruinRecordMaxIndex - 4): errorCountry,
        str(ruinRecordMaxIndex - 5): errorInBirthdayDate,
        str(ruinRecordMaxIndex - 6): errorInIssuanceDate
    }

    res = []
    global passportId
    for i in range(count):

        row = [passportId,
               extensions.fake.first_name(), extensions.fake.last_name(), random.choice(genders),
               extensions.fake.country() + ", " + extensions.fake.city(),
               extensions.random_date("1.1.1950", "1.1.1990", random.random()),
               extensions.random_date("1.1.2004", "1.1.2023", random.random())]

        fail_part = random.randrange(0, ruinRecordMaxIndex)
        if fail_part >= ruinRecordMinIndex:  # flag that we will fake
            index = ruinRecordMaxIndex - fail_part
            if fail_part <= ruinRecordMaxIndex - Fields.BIRTH.value:
                row[index] = dict_error[str(fail_part)](row[Fields.BIRTH.value])
            else:
                row[index] = dict_error[str(fail_part)]()

        res.append(row)

        passportId += 1

    return res
