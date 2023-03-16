import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()


def connect():
    db = mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        database="afim",
        password=os.getenv('DB_PASSWORD')
    )
    return db
