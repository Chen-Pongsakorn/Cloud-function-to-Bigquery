import base64
import datetime
import os
from bs4 import BeautifulSoup
import requests
from google.cloud import bigquery


class Config:
    dataset_id = os.environ.get("dataset_id")
    table_name = os.environ.get("table_name")
    url = os.environ.get("url")


def get_data(url):
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text,'html.parser')

    div = soup.find("div", {"id": "today-news"}).find_all("div", {"class": "gold"})

    rows = []

    for d in div:
        header = d.find('a').text
        source = d.find('span',{'class':'source'}).text
        date = d.find('span',{'class':'post-date'}).text
        rows.append((header, source, date))
        """ Get all header, source and date from div tag.
            Then add them into rows for the list of data"""

    return rows

def insert_data(event, context):
    client = bigquery.Client()
    dataset_ref = client.dataset(Config.dataset_id)
    
    record = get_data(Config.url)

    table_ref = dataset_ref.table(Config.table_name) 
    table = client.get_table(table_ref)
    result = client.insert_rows(table, record)
    return result
    