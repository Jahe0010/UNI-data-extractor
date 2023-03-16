import re

import requests
from bs4 import BeautifulSoup

from db.select import get_active_years, check_if_ongoing

from db.update import update_director_active


def crawl_active_years():
    for director in get_active_years():
        if check_if_ongoing("director", director["id"]):
            continue
        director["crawl_stage"] = "ongoing"
        update_director_active(director["id"], director["crawl_stage"], None, None)
        html_text = requests.get('https://en.wikipedia.org/wiki/' + director["name"].replace(" ", "_")).text

        infobox = BeautifulSoup(html_text, 'html.parser')
        infobox_label = infobox.find_all('th', class_='infobox-label')
        infobox_data = infobox.find_all('td', class_='infobox-data')
        
        if len(infobox_label) == 0:
          update_director_active(director["id"], "finished", None, None)

        active = None
        for element in infobox_label:
            if "Years" in element.text:
                index = infobox_label.index(element)
                infobox_data[index].text.strip().split("-")
                active = re.findall("([0-9]{4})", infobox_data[index].text)

        try:
            if active is not None:
                update_director_active(director["id"], "finished", active[0],
                                       active[1] if len(active) > 1 else None)
            else:
                update_director_active(director["id"], "finished", None, None)
        except Exception as e:
            update_director_active(director["id"], "finished", None, None)
        
