import requests
from db.select import get_location_list
from db.update import update_coordinates_for_location


def get_geo_response(city, region, country):
    query = city
    if region is not None:
        query = query + ",+" + region
    if country is not None:
        query = query + ",+" + country
    response = requests.get(f"https://nominatim.openstreetmap.org/search?q={query}&format=geojson&addressdetails=1")
    response_data = response.json()

    if len(response_data["features"]) > 0:
        geo_object = {
            "coordinates": str(response_data["features"][0]["geometry"]["coordinates"][0]) + "," + str(response_data["features"][0]["geometry"]["coordinates"][1])
        }

        return geo_object
    else:
        return


def load_coordinates_location():
    print("start iteration: -load_coordinates_location")
    for location in get_location_list():
        if "," in location["name"]:
            location["region"] = location["name"].split(",")[1]
            location["name"] = location["name"].split(",")[0]
        geo_object = get_geo_response(location["name"], location["region"], location["country"])

        if geo_object is not None:
            update_coordinates_for_location(location["id"], "finished", geo_object["coordinates"])
        else:
            update_coordinates_for_location(location["id"], "manual_to_refactor", None)
