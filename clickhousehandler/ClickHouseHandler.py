from clickhouse_driver import Client


class ClickHouseHandler:
    """Work with Clickhouse Database"""
    """Config - Clickhouse connection parameters"""

    def __init__(self, config):
        self.client = Client(**config)

    def executeNoReturnQuery(self, query):
        self.client.execute(query)

    def executeWithParams(self, query, params):
        self.client.execute(query, params)

    def executeReturnQuery(self, query):
        return self.client.execute(query).result_rows

    def __del__(self):
        self.client.disconnect()

    def close(self):
        self.client.disconnect()

    def test(self):
        res = self.executeReturnQuery("SELECT now()")
        print([i for i in res])


class ClickHouseSingleton:
    __instance = None

    def __init__(self):
        if ClickHouseSingleton.__instance:
            self.getInstance()

    @classmethod
    def getInstance(cls, config=None):
        if not cls.__instance:
            cls.__instance = ClickHouseHandler(config)
        return cls.__instance
