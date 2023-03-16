import schedule
import threading
import time
import queue

from crawler.crawlMovie import crawl_movie
from crawler.crawlDirector import crawl_director
from api.geoApi import load_coordinates_location
from crawler.crawlSchool import crawl_school
from crawler.crawlactive import crawl_active_years


def worker_movies():
    while True:
        job_func = jobqueue_movie.get()
        job_func()
        jobqueue_movie.task_done()


def worker_directors():
    while True:
        job_func = jobqueue_director.get()
        job_func()
        jobqueue_director.task_done()


def worker_coordinates():
    while True:
        job_func = jobqueue_coordinates.get()
        job_func()
        jobqueue_coordinates.task_done()


def worker_school():
    while True:
        job_func = jobqueue_school.get()
        job_func()
        jobqueue_school.task_done()


def worker_active_years():
    while True:
        job_func = jobqueue_active_years.get()
        job_func()
        jobqueue_active_years.task_done()


jobqueue_movie = queue.Queue()
jobqueue_director = queue.Queue()
jobqueue_coordinates = queue.Queue()
jobqueue_school = queue.Queue()
jobqueue_active_years = queue.Queue()

schedule.every(30).seconds.do(jobqueue_movie.put, crawl_movie)
schedule.every(30).seconds.do(jobqueue_director.put, crawl_director)
schedule.every(10).seconds.do(jobqueue_coordinates.put, load_coordinates_location)
schedule.every(30).seconds.do(jobqueue_school.put, crawl_school)
schedule.every(30).seconds.do(jobqueue_active_years.put, crawl_active_years)

worker_thread_movies = threading.Thread(target=worker_movies)
worker_thread_movies.start()

worker_thread_directors = threading.Thread(target=worker_directors)
worker_thread_directors.start()

worker_thread_coordinates = threading.Thread(target=worker_coordinates)
worker_thread_coordinates.start()

worker_thread_school = threading.Thread(target=worker_school)
worker_thread_school.start()

worker_thread_active_years = threading.Thread(target=worker_active_years)
worker_thread_active_years.start()

while True:
    schedule.run_pending()
    time.sleep(1)
