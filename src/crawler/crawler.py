import requests
from bs4 import BeautifulSoup

needed_headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
}


def crawler(url):
    website = requests.get(url, headers=needed_headers)
    results = BeautifulSoup(website.content, 'html.parser')
    return results
