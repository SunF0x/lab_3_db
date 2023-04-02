import mysql.connector


class MySQLHandler:
    """Work with MySQL Database"""
    """Config - MySQL connection parameters"""

    def __init__(self, config):
        self.connection = mysql.connector.connect(**config)
        self.cursor = self.connection.cursor()
        self.executeNoReturnQuery(f'use {config["database"]};')

    def executeNoReturnQuery(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def executeReturnQuery(self, query):
        self.cursor.execute(query)
        # self.executeNoReturnQuery(query)
        return self.cursor

    def __del__(self):
        self.connection.close()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def test(self):
        res = self.executeReturnQuery("SELECT now()")
        print([i for i in res])


class MySQLSingleton:
    __instance = None

    def __init__(self):
        if MySQLSingleton.__instance:
            self.getInstance()

    @classmethod
    def getInstance(cls, config=None):
        if not cls.__instance:
            cls.__instance = MySQLHandler(config)
        return cls.__instance
