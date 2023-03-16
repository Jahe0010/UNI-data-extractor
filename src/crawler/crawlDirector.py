import re

from crawler.crawler import crawler
from db.select import get_director_list, check_if_ongoing
from db.insert import insert_into_country, insert_into_location, insert_nicknames
from db.update import update_director_initial


def crawl_director():
    # search for the movie in the db and extract the link of the first result
    print("start iterating - intitial_crawl_director")
    for director in get_director_list():
        # first we set the crawl stage to start
        if check_if_ongoing("director", director["id"]):
            continue
        director["crawl_stage"] = "ongoing"
        update_director_initial(director)

        search_director_db = crawler("https://www.themoviedb.org/search/person?query=" + director["name"])
        director_result_link = search_director_db.find("a", class_="result", attrs={"data-media-type": "person"})
        director_url = director_result_link["href"]

        # extract information from movie db site
        director_site = crawler("https://www.themoviedb.org/" + director_url)

        # extract budget and revenue again
        info_container = director_site.find("section", class_="facts")
        info_container_paragraphs = info_container.findAll("p")

        location = {
            "name": None,
            "coordinates": None,
            "region": None,
            "crawl_stage": "manual_to_refactor"
        }

        country = {
            "name": ""
        }

        director["crawl_stage"] = "director_school"

        for paragraph in info_container_paragraphs:
            # try to set it
            if "Birthday" in paragraph.text:
                if not "-" in paragraph.text:
                    director["date_of_birth"] = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", paragraph.text)[0]

                if "Place of Birth" in paragraph.text:
                    if not "-" in paragraph.text:
                        cleaned_text = paragraph.text.split("Place of Birth ")[1].strip()
                        if len(cleaned_text.split(',').split(",")) > 2:
                            location["name"] = cleaned_text.split(',')[0].strip()
                            location["region"] = cleaned_text.split(',')[1].strip()
                            country["name"] = cleaned_text.split(',')[2].strip()
                        elif len(cleaned_text.split(',').split(",")) == 2:
                            location["name"] = cleaned_text.split(",")[0].strip()
                            country["name"] = cleaned_text.split(",")[1].strip().replace("[", "").replace("]", "")
                        location["crawl_stage"] = "location_coordinates"
                else:
                    place_of_birth_crawl = place_of_birth_director_crawl(director["name"])
                    if place_of_birth_crawl is not None and len(place_of_birth_crawl.split(",")) > 2:
                        location["name"] = place_of_birth_crawl.split(",")[0].strip()
                        location["region"] = place_of_birth_crawl.split(",")[1].strip()
                        location["crawl_stage"] = "location_coordinates"
                        country["name"] = place_of_birth_crawl.split(",")[2].strip()
                    elif place_of_birth_crawl is not None and len(place_of_birth_crawl.split(",")) > 1:
                        location["name"] = place_of_birth_crawl.split(",")[0].strip()
                        country["name"] = place_of_birth_crawl.split(",")[1].strip()
                        location["crawl_stage"] = "location_coordinates"

            # if theres a date of death we set it otherwise we skip
            if "Day of Death" in paragraph.text:
                director["date_of_death"] = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", paragraph.text)[0].strip()
            # if gender is in text we take it otherwise we skip
            if "Gender" in paragraph.text:
                director["sex"] = paragraph.text.split()[1].strip()

        nicknames = []
        for knownAs in info_container.findAll("li", {"itemprop": "additionalName"}):
            nicknames.append(knownAs.text.strip())

        if not director["date_of_birth"]:
            director["date_of_birth"] = date_of_birth_director_crawl(director["name"])

        country["name"] = extract_country_mapping(country["name"])
        if country["name"] and location["name"]:
            insert_into_country(country["name"])
            location_id = insert_into_location(location, country["name"])
            if location_id:
                director["place_of_birth"] = location_id

        # we insert the director
        update_director_initial(director)

        # we insert the director nicknames
        for nickname in nicknames:
            insert_nicknames(director["id"], nickname)


def date_of_birth_director_crawl(director_name: str):
    director_site = crawler("https://en.wikipedia.org/wiki/" + director_name.replace(" ", "_"))
    info_container = director_site.find("span", class_="bday")

    date_of_birth = None
    if info_container:
        date_of_birth = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", info_container.text)

    if date_of_birth is not None:
        return date_of_birth[0]
    else:
        return None


def place_of_birth_director_crawl(director_name: str):
    director_site = crawler("https://en.wikipedia.org/wiki/" + director_name.replace(" ", "_"))
    info_container = director_site.find("div", class_="birthplace")
    birth_place_string = None

    if info_container:
        birth_place_string = info_container.text

    return birth_place_string


def extract_country_mapping(country: str):
    mapping = {
        "United States": ["US", "USA", "U.S.A", "U.S.", "U.S"],
        "United Kingdom": ["UK"],
        "Japan": ["Empire of Japan"],
        "Italy": ["Kingdom of Italy"]
    }
    for element in mapping:
        if country in mapping[element]:
            return element

    return country
