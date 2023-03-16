from db.connector import connect


def update_movie(movie):
    db = connect()
    cursor = db.cursor()
    sql = "Update movies SET budget = %s, fsk = %s, money_earned = %s, released =%s, runtime=%s, crawl_stage=%s  WHERE id = %s"
    cursor.execute(sql, (
        movie["budget"], movie["fsk"], movie["revenue"], movie["released"], movie["runtime"], movie["crawl_stage"],
        movie["id"]))

    db.commit()
    return


def update_director_initial(director):
    db = connect()
    cursor = db.cursor()

    sql = "Update director SET date_of_birth = %s, school = %s, active_since = %s, place_of_birth = %s, sex = %s, date_of_death = %s, crawl_stage=%s, active_until = %s  WHERE id = %s"
    cursor.execute(sql, (
        director["date_of_birth"], director["school"], director["active_since"], director["place_of_birth"],
        director["sex"], director["date_of_death"], director["crawl_stage"], director["active_until"], director["id"]))

    db.commit()
    return


def update_coordinates_for_location(location_id, crawl_stage, location_info):
    db = connect()
    cursor = db.cursor()

    sql = "Update location SET coordinates = %s, crawl_stage=%s  WHERE id = %s"
    cursor.execute(sql, (location_info, crawl_stage, location_id))

    db.commit()
    return


def update_director_school(director_id, crawl_stage, school_id):
    db = connect()
    cursor = db.cursor()
    sql = "Update director SET school = %s, crawl_stage = %s  WHERE id = %s"
    cursor.execute(sql, (school_id, crawl_stage, director_id))

    db.commit()
    return


def update_director_active(director_id, crawl_stage, active_since, active_until):
    db = connect()
    cursor = db.cursor()
    sql = "Update director set active_since = %s, active_until = %s, crawl_stage = %s where id = %s"
    cursor.execute(sql, (active_since, active_until, crawl_stage, director_id))

    db.commit()
    return
