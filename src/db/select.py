from db.connector import connect

"""
Can handle multiple values and returns an id if the value already exists
"""


def check_if_exist_and_return_id(table, values):
    db = connect()
    cursor = db.cursor()

    sql = f"Select id From {table} Where "
    for value in values:
        if list(values)[-1] == value:
            if isinstance(values[value], int):
                sql += f"{value} = {values[value]} "
            else:
                sql += value + " = '" + values[value].replace("'", "''") + "' "
        else:
            if isinstance(values[value], int):
                sql += f"{value} = {values[value]} AND "
            else:
                sql += value + " = '" + values[value].replace("'", "''") + "' AND "
    cursor.execute(sql)
    row_id = cursor.fetchone()

    if row_id:
        return row_id[0]
    else:
        return row_id


def get_movie_list():
    db = connect()
    cursor = db.cursor()
    sql = "select * from movies where crawl_stage='movie_initial'"
    cursor.execute(sql)
    result = cursor.fetchall()
    result_list = []

    for result_item in result:
        extract_year = None
        if result_item[2]:
            transform_to_object = result_item[2]
            extract_year = transform_to_object.year

        movie_object = {
            "id": result_item[0],
            "title": result_item[1],
            "year": extract_year,
            "released": result_item[2],
            "imdb_rating": result_item[3],
            "revenue": result_item[4],
            "budget": result_item[5],
            "runtime": result_item[6],
            "fsk": result_item[7],
            "crawl_stage": result_item[8]
        }
        result_list.append(movie_object)

    return result_list


def get_location_list():
    db = connect()
    cursor = db.cursor()
    sql = "select id, name, region, country from location where crawl_stage='location_coordinates'"
    cursor.execute(sql)
    result = cursor.fetchall()
    result_list = []
    for result_item in result:
        director_object = {
            "id": result_item[0],
            "name": result_item[1],
            "region": result_item[2],
            "country": result_item[3]
        }
        result_list.append(director_object)
    return result_list


def get_director_list():
    db = connect()
    cursor = db.cursor()
    sql = "select * from director where crawl_stage='director_date_of_birth'"
    cursor.execute(sql)
    result = cursor.fetchall()
    result_list = []

    for result_item in result:
        director_object = {
            "id": result_item[0],
            "name": result_item[1],
            "date_of_birth": result_item[2],
            "school": result_item[3],
            "active_since": result_item[4],
            "place_of_birth": result_item[5],
            "sex": result_item[6],
            "date_of_death": result_item[7],
            "crawl_stage": result_item[8],
            "active_until": result_item[9]
        }
        result_list.append(director_object)
    return result_list


def check_if_ongoing(database, item_id):
    db = connect()
    cursor = db.cursor()
    sql = f"select crawl_stage from {database} where id='{item_id}'"
    cursor.execute(sql)
    result = cursor.fetchone()
    if result[0] == "ongoing":
        return True
    else:
        return False


def get_director_school():
    db = connect()
    cursor = db.cursor()
    sql = "select id, name from director where crawl_stage='director_school'"
    cursor.execute(sql)
    result = cursor.fetchall()
    result_list = []
    for director in result:
        director_item = {
            "id": director[0],
            "name": director[1]
        }
        result_list.append(director_item)

    return result_list


def get_active_years():
    db = connect()
    cursor = db.cursor()
    sql = "select id, name from director where crawl_stage = 'director_active'"
    cursor.execute(sql)
    result = cursor.fetchall()
    result_list = []
    for director in result:
        director_item = {
            "id": director[0],
            "name": director[1]
        }
        result_list.append(director_item)

    return result_list
