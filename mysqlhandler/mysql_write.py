from mysqlhandler import MySQLHandler
from passportGenerator.passportGeneration import generatePassportData
import json


def migration(mysql_instance: MySQLHandler,
              mysql_prop: json,
              row_count: int):
    # creating table stage
    createTable(mysql_instance, mysql_prop)
    # insert data
    initialInsert(mysql_instance, mysql_prop, row_count)


def createTable(mysql_instance: MySQLHandler,
                mysql_prop: json):
    table_name = mysql_prop["table"]
    mysql_instance.executeNoReturnQuery(f'DROP TABLE IF EXISTS {table_name}')

    create_query = f'CREATE TABLE {table_name}('
    for field in mysql_prop["table_structure"]:
        create_query += f'{field} {mysql_prop["table_structure"][field]}, '  # field name and type

    primary_key = mysql_prop["primary_key"]

    if primary_key:
        create_query += f'PRIMARY KEY ({primary_key})'

    mysql_instance.executeNoReturnQuery(create_query + ");")


def initialInsert(mysql_instance, mysql_prop, row_count):
    generated_data = generatePassportData(row_count)

    field_count = len(mysql_prop["table_structure"])
    fields = ','.join([field for field in mysql_prop["table_structure"]])
    insert_query = f'INSERT INTO {mysql_prop["table"]} ({fields}) VALUES '
    formatted_insert_row = ("(" + "{}," * field_count)[:-1] + ")"

    for record in generated_data:
        insert_query += formatted_insert_row.format(*formatString(record)) + ","

    mysql_instance.executeNoReturnQuery(insert_query[:-1])


def formatString(record):
    res = tuple()
    for field_value in record:
        if isinstance(field_value, str):
            field_value = "\"" + field_value + "\""
        res += (field_value, )

    return res
