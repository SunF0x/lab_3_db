from mysqlhandler.MySQLHandler import MySQLSingleton
from pghandler.PgHandler import PgSingleton
from mysqlhandler import mysql_write, mysql_read
from clickhousehandler import ClickHouseHandler, clickhouse_write
from pymongo import MongoClient
import mongohandler.mongoHandler

from pghandler import pg_write, pg_read

import json

propertiesFileName = "../databaseProperties.json"
propertiesFileJson = json.load(open("../databaseProperties.json"))
metaFileJson = json.load(open("../meta.json"))


def mysqlStage():
    mysqlInstance = MySQLSingleton.getInstance(propertiesFileJson["mysql"])
    # migrate (init)
    mysql_write.migration(mysqlInstance, metaFileJson["mysql"], metaFileJson["row_count"])
    # read, fix and get inserted values
    mysqlHandledRecords = mysql_read.prepare_values(mysqlInstance, metaFileJson["mysql"])
    mysqlInstance.close()

    return mysqlHandledRecords


def mongoStage():
    mongo_props = propertiesFileJson["mongo"]
    client = MongoClient(f"mongodb://{mongo_props['host']}:{mongo_props['port']}")
    mongohandler.mongoHandler.migrate(client, metaFileJson["row_count"], metaFileJson["mongo"])
    mongo_records = mongohandler.mongoHandler.mongo_read(client, metaFileJson["mongo"])
    client.close()

    return mongo_records


def pgStage(mysql_records, mongo_records):
    pgInstance = PgSingleton.getInstance(propertiesFileJson["postgres"])
    # migrate (init)
    pg_write.migration(pgInstance, metaFileJson["postgres"], mysql_records, mongo_records)
    # mysqlHandledRecords = mysql_read.prepare_values(pgInstance, metaFileJson["postgres"])
    queries_result = pg_read.make_queries(pgInstance, metaFileJson["queries_params"])
    pgInstance.close()

    return queries_result


def chStage(records):
    chInstance = ClickHouseHandler.ClickHouseSingleton.getInstance(propertiesFileJson["clickhouse"])
    # migrate (init)
    clickhouse_write.migration(chInstance, records, metaFileJson["clickhouse"])
    chInstance.close()

    return


# migrations + records fixing
mysqlRecords = mysqlStage()
mongoRecords = mongoStage()

# migrate for pg and write records from prev stages
pg_queries = pgStage(mysqlRecords, mongoRecords)
# insert queries in clickhouse
chStage(pg_queries)

