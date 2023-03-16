import requests
from bs4 import BeautifulSoup

from db.select import get_director_school, check_if_ongoing
from db.insert import insert_into_school, insert_into_country, insert_into_location

from db.update import update_director_school


def crawl_school():
    for director in get_director_school():
        # first we set the crawl stage to start
        if check_if_ongoing("director", director["id"]):
            continue
        director["crawl_stage"] = "ongoing"
        update_director_school(director["id"], director["crawl_stage"], None)

        html_text = requests.get('https://en.wikipedia.org/wiki/' + director["name"].replace(" ", "_")).text

        soup = BeautifulSoup(html_text, 'html.parser')
        info_box = soup.find_all('td', class_='infobox-data')

        school = {}
        links = []
        for field in info_box:
            link = field.find_all('a')
            if len(link) > 0:
                links.append(link[0])

        university_link = None
        for link in links:
            if "university" in link.text.lower():
                university_link = link
            if "school" in link.text.lower():
                university_link = link

        if university_link is not None:
            school["name"] = university_link.text.split(",")[0]
            school["location"] = {}
            try:
                school_text = requests.get('https://en.wikipedia.org/' + university_link["href"]).text

                school_soup = BeautifulSoup(school_text, 'html.parser')
                school["location"]["crawl_stage"] = "finished"

                try:
                    school["location"]["coordinates"] = school_soup.find("span", class_='geo').text
                    if ";" in school["location"]["coordinates"]:
                        school["location"]["coordinates"].replace("; ", ",")
                except Exception as e:
                    school["location"]["coordinates"] = None
                    school["location"]["crawl_stage"] = "location_coordinates"

                try:
                    school["location"]["country"] = school_soup.find("div", class_='country-name').text
                except Exception as e:
                    school["location"]["country"] = None

                try:
                    school["location"]["region"] = school_soup.find("div", class_='region').text
                except Exception as e:
                    school["location"]["region"] = None

                try:
                    school["location"]["name"] = school_soup.find("div", class_='locality').text
                except Exception as e:
                    school["location"]["name"] = None

                if school["location"]["country"] is not None:
                    insert_into_country(school["location"]["country"])
                if school["location"]["name"] is not None:
                    school["location"] = insert_into_location(school["location"], school["location"]["country"])

                school["crawl_stage"] = "finished"
                school_id = insert_into_school(school)
                update_director_school(director["id"],  "director_active", school_id)

            except Exception as e:
                # insert into db -> school
                school["location"] = None
                school["crawl_stage"] = "finished"
                school_id = insert_into_school(school)
                # update director -> schoolid, director id
                update_director_school(director["id"], "director_active", school_id)
        else:
            update_director_school(director["id"], "director_active", None)
