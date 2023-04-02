import json
import re
from datetime import date, datetime
from dateutil import relativedelta
from dateutil.relativedelta import relativedelta
import passportGenerator.passportGeneration
from mysqlhandler import MySQLHandler
from passportGenerator.extensions import translateCountryFromEnToRu, translateGenderFromEnToRu, \
    translateLastNameFromEnToRu


def prepare_values(mysql_instance: MySQLHandler,
                   mysql_prop: json):
    res = []
    # read record
    # check and fix all field
    #   if error in field -> add it log

    records = mysql_instance.executeReturnQuery(f"SELECT * FROM {mysql_prop['table']}")

    logfile = open("mysql_logfile.txt", "w")
    logfile.write(f"Log session <MySQL> {date.today()}\n")

    for record in records:
        res.append(fixRecord(record, logfile))

    logfile.close()
    return res


# ruinRecordMaxIndex-1 - error in last name // имя на английском - в русское, добавляются символы пунктуации
# ruinRecordMaxIndex-2 - error in first name // имя на английском - в русское, добавляются символы пунктуации
# ruinRecordMaxIndex-3 - error in sex // пол из слова на английском - перевести на русский
# ruinRecordMaxIndex-4 - error in address (fake) // страна на английском - перевести на русский
# ruinRecordMaxIndex-5 - error in date // год был 1920 - стал 20, т.е. надо добавить в начало 19
# ruinRecordMaxIndex-6 - error in passport issuance date // дата паспорта < 14, т.е. посчитать разницу с текущей датой и
# в зависимости от возраста прибавить к дате рождения либо 14, если младше 20, либо 20, если младше 45, иначе 45


def fixRecord(record, logfile):
    initial_record = record
    items = list(record)
    reasons = []

    # fix last name
    lastname_index = passportGenerator.passportGeneration.Fields.LASTNAME.value
    items[lastname_index] = re.sub(r'[^\w\s]', '', items[lastname_index])
    updated_value = translateLastNameFromEnToRu(items[lastname_index])
    if updated_value:
        reasons.append("Last name")
        items[lastname_index] = updated_value

    # fix first name
    firstname_index = passportGenerator.passportGeneration.Fields.FIRSTNAME.value
    items[firstname_index] = re.sub(r'[^\w\s]', '', items[firstname_index])
    updated_value = translateLastNameFromEnToRu(items[firstname_index])
    if updated_value:
        reasons.append("First name")
        items[firstname_index] = updated_value

    # fix gender
    gender_index = passportGenerator.passportGeneration.Fields.GENDER.value
    updated_value = translateGenderFromEnToRu(items[gender_index])
    if updated_value:
        reasons.append("Gender")
        items[gender_index] = updated_value

    # fix address
    address_index = passportGenerator.passportGeneration.Fields.ADDRESS.value
    updated_value = translateCountryFromEnToRu(items[address_index])
    if updated_value:
        reasons.append("Address")
        items[address_index] = updated_value

    # fix birthday
    birth_index = passportGenerator.passportGeneration.Fields.BIRTH.value
    birth_value_slp = items[birth_index].split('.')
    if 0 <= int(birth_value_slp[2]) < 100:
        reasons.append("Birthday date")
        birth_value_slp[2] = "19" + birth_value_slp[2]
        items[birth_index] = '.'.join(birth_value_slp)

    # fix passport issue date
    # WRONG
    # разн = посчитать разницу между датой рождения и паспорта
    # если разн < 14
    #   посчитать возраст = сегодня - дата рождения
    #       если возраст <= 20
    #           дата выдачи паспорта = дата рождения + 14 лет
    #       иначе если возраст < 45
    #           дата выдачи паспорта = дата рождения + 20 лет
    #       иначе
    #           дата выдачи паспорта = дата рождения + 45
    passport_issue_index = passportGenerator.passportGeneration.Fields.PASS_ISSUE.value
    passport_issue_date = datetime.strptime(items[passport_issue_index], "%d.%m.%Y").date()
    birthday_date = datetime.strptime(items[birth_index], "%d.%m.%Y").date()
    diff = relativedelta(passport_issue_date, birthday_date)
    if diff.years < 14:
        reasons.append("Passport issue date")

        age = relativedelta(birthday_date, date.today())
        append_year = 14 if age.years < 20 else 20 if age.years < 45 else 45
        items[passport_issue_index] = add_years(birthday_date, append_year)

    if reasons:
        logfile.write(f"Error in record: {initial_record}\n")
        for reason in reasons:
            logfile.write(f"Reason: \t {reason}\n\n")

    return items


def add_years(start_date, years):
    try:
        return start_date.replace(year=start_date.year + years)
    except ValueError:
        # 👇️ preserve calendar day (if Feb 29th doesn't exist, set to 28th)
        return start_date.replace(year=start_date.year + years, day=28)
