from db.connector import connect
from db.select import check_if_exist_and_return_id
from db.update import update_director_initial


def insert_into_location(location, country):
    if location["name"] is not None:
        db = connect()
        cursor = db.cursor()

        sql = "INSERT IGNORE INTO location ( name, coordinates, country, region, crawl_stage) VALUES (%s, " \
              "%s, %s, %s, %s) "
        cursor.execute(sql, (
            location["name"], location["coordinates"], country, location["region"], location["crawl_stage"]))

        db.commit()
        return cursor.lastrowid
    else:
        return None


def insert_into_genre(name):
    db = connect()
    cursor = db.cursor()

    sql = "INSERT IGNORE INTO genre (name) VALUES (%s)"
    cursor.execute(sql, (name,))

    db.commit()
    return


def insert_into_country(name):
    db = connect()
    cursor = db.cursor()

    sql = "INSERT IGNORE INTO country (name) VALUES (%s)"
    cursor.execute(sql, (name,))

    db.commit()
    return


def insert_into_fsk(name):
    db = connect()
    cursor = db.cursor()

    sql = "INSERT IGNORE INTO fsk (name) VALUES (%s)"
    cursor.execute(sql, (name,))

    db.commit()
    return


def insert_into_director(director, location_id):
    db = connect()
    cursor = db.cursor()
    check_values = {
        "name": director["name"]
    }

    if "date_of_birth" in director and director["date_of_birth"]:
        check_values["date_of_birth"] = director["date_of_birth"]

    if "place_of_birth" in director and director["place_of_birth"]:
        check_values["place_of_birth"] = director["place_of_birth"]

    row_id = check_if_exist_and_return_id("director", check_values)

    if row_id is None:
        sql = "INSERT IGNORE INTO director (place_of_birth, date_of_birth, name, school, active_since, crawl_stage) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (
            location_id, director["date_of_birth"], director["name"].replace("'", "\'"), director["school"],
            director["active_since"], director["crawl_stage"]))

        db.commit()
        return cursor.lastrowid
    else:
        director["id"] = row_id
        update_director_initial(director)
        return row_id


def insert_nicknames(director, nickname):
    db = connect()
    cursor = db.cursor()
    check_values = {
        "director": director,
        "name": nickname
    }
    row_id = check_if_exist_and_return_id("nicknames", check_values)

    if row_id is None:
        sql = "INSERT IGNORE INTO nicknames (director, name) VALUES (%s, %s)"

        cursor.execute(sql, (director, nickname))
        db.commit()
    return


def insert_into_movies(movie, fsk):
    db = connect()
    cursor = db.cursor()
    check_values = {
        "titel": movie["title"],
        "released": movie["released"]
    }

    row_id = check_if_exist_and_return_id("movies", check_values)

    if row_id is None:
        sql = "INSERT IGNORE INTO movies (titel, released, imdb_rating, money_earned, budget, runtime, fsk, crawl_stage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (
            movie["title"].replace("'", "\'"), movie["released"], movie["imdb_rating"], int(movie["money_earned"]),
            movie["budget"],
            movie["runtime"], fsk, movie["crawl_stage"]))

        db.commit()
        return cursor.lastrowid
    else:
        return row_id


def insert_into_mrg(movie, director, genre):
    db = connect()
    cursor = db.cursor()
    sql = "INSERT IGNORE INTO MRG (movie, director, genre) VALUES (%s, %s, %s)"
    cursor.execute(sql, (movie, director, genre))

    db.commit()
    return


def insert_into_school(school):
    db = connect()
    cursor = db.cursor()
    sql = "INSERT INTO school (name, locationId, crawl_stage) VALUES (%s, %s, %s)"
    cursor.execute(sql, (school["name"], school["location"], school["crawl_stage"]))

    db.commit()
    return cursor.lastrowid



