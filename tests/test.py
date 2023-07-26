
# import psycopg2

# def check_lot_number_exists(lot_number):
#     conn = psycopg2.connect(database="postgres", user="postgres", password="12345678", host="localhost", port="5432")

#     try:
#         cur = conn.cursor()
#         cur.execute("SELECT EXISTS(SELECT 1 FROM tenders WHERE lot_number = %s)", (lot_number,))
#         result = cur.fetchone()[0]
#         return result
#     except (Exception, psycopg2.DatabaseError) as error:
#         print("Error while executing SQL query:", error)
#         return False
#     finally:
#         if conn is not None:
#             conn.close()

# if check_lot_number_exists('62215895-ОК1'):
#     print('Not adding to DB')
# else:
#     print("Adding")
import requests

from bs4 import BeautifulSoup
# response = requests.get('https://goszakup.gov.kz/ru/announce/index/10365832?tab=lots#', verify=False).text
# soup = BeautifulSoup(response, 'html.parser')
# table = soup.find('table', class_='table-bordered')
# data_lot_ids = []
# links = table.find_all('a', class_='btn-select-lot')
# for link in links:
#     data_lot_id = link.get('data-lot-id')
#     data_lot_ids.append(data_lot_id)
# print(data_lot_ids)
data = {
    'id': '62206385',
}
import time
from bs4 import BeautifulSoup
import requests
import sys
import io
import sqlite3
import multiprocessing
import urllib3
from fake_useragent import UserAgent
import psycopg2
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print(requests.post(
    f'https://goszakup.gov.kz/ru/announce/ajax_load_lot/10368317?tab=lots', data=data, verify=False).text)
