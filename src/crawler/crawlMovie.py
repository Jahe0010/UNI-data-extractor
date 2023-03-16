import datetime
import re

from crawler.crawler import crawler
from db.select import get_movie_list, check_if_ongoing
from db.insert import insert_into_mrg, insert_into_director, insert_into_fsk, insert_into_genre
from db.update import update_movie


# filter for movie information
def crawl_movie():
    # search for the movie in the db and extract the link of the first result
    print("start iterating: - crawl_movie")
    for movie in get_movie_list():
        # first we set the crawl stage to start
        if check_if_ongoing("movies", movie["id"]):
            continue
        movie["crawl_stage"] = "ongoing"
        update_movie(movie)
        url = "https://www.themoviedb.org/search/movie?query=" + movie["title"] + " y:" + str(movie["year"])
        search_movie_db = crawler(url)
        movie_result_link = search_movie_db.find("a", class_="result", attrs={"data-media-type": "movie"})
        try:
            movie_url = movie_result_link["href"]
            if not len(movie_url) > 0:
                movie["crawl_stage"] = "manual_to_refactor"
                update_movie(movie)
                continue
        except Exception as e:
            movie["crawl_stage"] = "manual_to_refactor"
            update_movie(movie)
            continue

        # extract information from movie db site
        movie_site = crawler("https://www.themoviedb.org/" + movie_url)

        movie["crawl_stage"] = "finished"
        # fsk when there is absolutly no fsk we set it to a very high number (invalid)
        try:
            if movie_site.find("span", class_="certification"):
                fsk = movie_site.find("span", class_="certification").text.replace(" ", "").replace('\n', '').replace(
                    '\r',
                    '')
            else:
                fsk = None
        except Exception as e:
            fsk = None

        # runtime
        try:
            movie["runtime"] = movie_site.find("span", class_="runtime").text.strip()
        except Exception as e:
            print("no runtime found")

        # released
        released_text = movie_site.find("span", class_="release").text.split("(")[0].strip()
        released_date = datetime.datetime.strptime(released_text, "%m/%d/%Y")
        released = released_date.strftime("%Y-%m-%d")

        # extract the genres
        try:
            genres = movie_site.find("span", class_="genres").text.strip().split(",")
        except Exception as e:
            genres = []
            print(e)

        # extract the director again (we doublecheck the execel!
        profiles = movie_site.findAll("li", class_="profile")
        directors = []
        for profile in profiles:
            if "Director" in profile.text:
                for link in profile.findAll("a"):
                    directors.append(link.text.replace('\n', '').replace('\r', '').replace('\xa0', ''))

        # extract budget and revenue again
        info_container = movie_site.find("section", class_="facts")
        info_container_paragraphs = info_container.findAll("p")
        budget = None
        revenue = None
        try:
            for paragraph in info_container_paragraphs:
                if "Budget" in paragraph.text:
                    budget = re.search("\d+\.\d+", paragraph.text.replace(",", ""))[0]
                if "Revenue" in paragraph.text:
                    revenue = re.search("\d+\.\d+", paragraph.text.replace(",", ""))[0]
        except Exception as e:
            revenue = None
            budget = None

        movie["budget"] = budget
        movie["fsk"] = fsk
        movie["revenue"] = revenue
        movie["released"] = released

        insert_into_fsk(fsk)
        update_movie(movie)

        for genre in genres:
            insert_into_genre(genre.strip())
            for director in directors:
                director_object = {
                    "name": director,
                    "date_of_birth": None,
                    "school": None,
                    "active_since": None,
                    "active_until": None,
                    "place_of_birth": None,
                    "date_of_death": None,
                    "sex": None,
                    "crawl_stage": "director_date_of_birth"
                }
                director_id = insert_into_director(director_object, None)
                insert_into_mrg(movie["id"], director_id, genre)
