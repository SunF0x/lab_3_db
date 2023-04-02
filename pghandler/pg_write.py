from pghandler import PgHandler


def migration(pg_instance: PgHandler, pg_meta, mysql_records, mongo_records):
    tables = pg_meta["tables"]
    pg_instance.executeNoReturnQuery("SET DATESTYLE = German")  # for date formal dd.mm.yyyy
    for table in tables:
        pg_instance.executeNoReturnQuery(f"DROP TABLE IF EXISTS {table['table']} CASCADE")

    index = 0
    table_with_pk = 0
    table_with_fk = 0

    for table in tables:
        structure = table["table_structure"]
        create_query = f"CREATE TABLE {table['table']} ("
        for key in structure:
            create_query += f"{key} {structure[key]}, "

        if 'primary_key' in table:
            table_with_pk = index
            create_query += f"PRIMARY KEY ({table['primary_key']}))"
        else:
            create_query = create_query[:-2] + ")"

        if 'FK' in table:
            table_with_fk = index
            create_query += f'; ALTER TABLE {table["table"]} ADD CONSTRAINT fk_{table["table"]}_{table["FK"]["foreign"]["table"]} ' \
                            f'FOREIGN KEY ({table["FK"]["local"]}) REFERENCES {table["FK"]["foreign"]["table"]} ({table["FK"]["foreign"]["attribute"]});'

        index += 1
        pg_instance.executeNoReturnQuery(create_query)

    table_pk = tables[table_with_pk]
    first_insert_query = f"INSERT INTO {table_pk['table']} VALUES "

    format_string = makeFormatString(tables, table_with_pk)

    src_with_pk = mysql_records if table_pk['from'] == 'mysql' else mongo_records
    src_with_fk = mongo_records if src_with_pk == mysql_records else mysql_records

    # first insert with pk
    for record in src_with_pk:
        first_insert_query += format_string.format(*record) + ","

    pg_instance.executeNoReturnQuery(first_insert_query[:-1] + ";")

    table_fk = tables[table_with_fk]
    # then with fk
    first_insert_query = f"INSERT INTO {table_fk['table']} VALUES "

    format_string = makeFormatString(tables, table_with_fk)

    for record in src_with_fk:
        first_insert_query += format_string.format(*record) + ","

    pg_instance.executeNoReturnQuery(first_insert_query[:-1])

    return


def makeFormatString(tables, table_index):
    table_with_k = tables[table_index]
    structure_fk = table_with_k["table_structure"]

    format_string = "("
    for key in structure_fk:
        format_string += pg_type_format[structure_fk[key].lower()] + ","

    return f"{format_string[:-1]})"


pg_type_format = {
    "int": "{}",
    "serial": "{}",
    "date": "\'{}\'",
    "varchar": "\'{}\'",
    "timestamp": "TO_TIMESTAMP('{}','YYYY-MM-DD HH24:MI:SS')",
    "money": "{}",
    "bigint": "{}"
}
