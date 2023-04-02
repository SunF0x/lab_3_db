from pghandler import PgHandler


def make_queries(pg_instance: PgHandler, query_meta):
    # 1. Те, кто летали с A по Б
    # 2. Те, кто праздновал др в в самолете
    # 3. Группы людей, кто купил билеты в промежутках цен
    res = [[row for row in from_date_to_date_query(pg_instance, query_meta[0])],
           [row for row in birthday_flight_in_town_query(pg_instance, query_meta[1])],
           [row for row in price_group_data(pg_instance, query_meta[2])]]

    return res


def from_date_to_date_query(pg_instance: PgHandler, query_meta):
    fields_to_select = get_query_fields(query_meta)
    flight_between_query = f"SELECT {fields_to_select} FROM passport p JOIN flight f ON p.passport_id = f.passport " \
                           f"WHERE f.date_from BETWEEN '{query_meta['flight_between'][0]}' " \
                           f"AND '{query_meta['flight_between'][1]}';"

    return pg_instance.executeReturnQuery(flight_between_query)


def get_query_fields(query_meta):
    fields_to_select = ""
    if "passport_fields" in query_meta:
        for passport_field in query_meta["passport_fields"]:
            fields_to_select += f"p.{passport_field}, "

    if "flight_fields" in query_meta:
        for flight_field in query_meta["flight_fields"]:
            fields_to_select += f"f.{flight_field}, "

    if fields_to_select == "":
        fields_to_select = "*  "

    return fields_to_select[:-2]


def birthday_flight_in_town_query(pg_instance: PgHandler, query_meta):
    fields_to_select = get_query_fields(query_meta)

    flight_between_query = f"SELECT {fields_to_select} FROM passport p JOIN flight f ON p.passport_id = f.passport " \
                           f"WHERE EXTRACT(MONTH FROM p.birthday) BETWEEN EXTRACT(MONTH FROM f.date_from) AND EXTRACT(MONTH FROM f.date_to) " \
                           f"AND EXTRACT(DAY FROM p.birthday) BETWEEN EXTRACT(DAY FROM f.date_from) AND EXTRACT(DAY FROM f.date_to)"

    if "birthday_in" in query_meta:
        flight_between_query += f" AND f.to_town = '{query_meta['birthday_in']}'"

    return pg_instance.executeReturnQuery(flight_between_query)


def price_group_data(pg_instance: PgHandler, query_meta):
    fields_to_select = get_query_fields(query_meta)

    ranges = ""
    for price_range in query_meta["price_between"]:
        ranges += f"f.price BETWEEN {price_range['start']} AND {price_range['stop']} OR "

    ranges = ranges[:-3]

    query = f"SELECT {fields_to_select} FROM passport p JOIN flight f ON p.passport_id = f.passport " \
            f"WHERE {ranges} ORDER BY f.price;"

    return pg_instance.executeReturnQuery(query)
