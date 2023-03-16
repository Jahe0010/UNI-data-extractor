import pandas as pd

from db.insert import insert_into_director, insert_into_movies, insert_into_genre, insert_into_mrg


def extract_data_from_csv(path, columns_to_drop):
    df = pd.read_csv(path)
    new_df = df.drop(columns=columns_to_drop)

    return new_df


data_to_remove_from_dataset = ["Poster_Link", "Certificate", "Overview", "Meta_score", "Star1", "Star2", "Star3",
                               "Star4", "No_of_Votes"]


# Function to handel movie stuff
def movie(df, ind):
    movie_object = {
        "title": df["Series_Title"][ind],
        "released": df["Released_Year"][ind] + "-01-01",
        "imdb_rating": df["IMDB_Rating"][ind],
        "crawl_stage": "movie_initial",
        "money_earned": df["Gross"][ind],
        "budget": None,
        "runtime": df["Runtime"][ind]
    }
    movie_id = insert_into_movies(movie_object, None)
    return movie_id


def run_csv_import():
    df = extract_data_from_csv("../imdb_top_1000.csv", data_to_remove_from_dataset)
    print("Einträge:")

    for ind in df.index:
        print(ind)
        movie(df, ind)
    return
