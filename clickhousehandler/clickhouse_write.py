from clickhousehandler import ClickHouseHandler


def migration(clickhouse: ClickHouseHandler, records, ch_meta):
    # drop tables
    for table in ch_meta["tables"]:
        clickhouse.executeNoReturnQuery(f"DROP TABLE IF EXISTS {table['table']}")

    # create tables
    for table in ch_meta["tables"]:
        structure = table['table_structure']
        create_query = f"CREATE TABLE {table['table']} ("
        for field in structure:
            create_query += f"{field} {structure[field]},"
        create_query = create_query[:-1] + ") ENGINE = Memory"
        clickhouse.executeNoReturnQuery(create_query)

    for table_index in range(len(ch_meta["tables"])):
        current_table = ch_meta["tables"][table_index]
        data = make_dict_from_list(current_table["table_structure"], records[table_index])
        query = f'INSERT INTO {current_table["table"]} ({",".join([key for key in current_table["table_structure"]])}) VALUES '
        clickhouse.executeWithParams(query, data)

    return


def make_dict_from_list(ch_table_structure, data):
    dict_keys = [key for key in ch_table_structure]

    res = []
    for index in range(len(data)):
        res.append(dict(zip(dict_keys, data[index])))

    return res
