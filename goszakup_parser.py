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

conn = psycopg2.connect(database="postgres", user="postgres",
                        password="postgres", host="localhost", port="5432")
cursor = conn.cursor()
file = open('logs.txt', 'w', encoding='utf-8')
# Create the "tenders" table
create_table_query = '''
CREATE TABLE IF NOT EXISTS tenders (
    lot_name TEXT,
    customer TEXT,
    lot_description TEXT,
    quantity TEXT,
    price TEXT,
    procurement_method TEXT,
    status TEXT,
    url_to_details TEXT,
    lot_number TEXT,
    start_date TEXT,
    end_date TEXT,
    customer_bin TEXT,
    customer_name TEXT,
    ktru_code TEXT,
    ktru_name TEXT,
    brief_characteristics TEXT,
    additional_characteristics TEXT,
    funding_source TEXT,
    unit_price TEXT,
    unit_measurement TEXT,
    year1_amount TEXT,
    year2_amount TEXT,
    year3_amount TEXT,
    planned_amount TEXT,
    advance_payment_percentage TEXT,
    delivery_location_kato TEXT,
    kato TEXT,
    tru_delivery_period TEXT,
    incoterms TEXT,
    lot_url TEXT,
    sended_to TEXT
);
'''
cursor.execute(create_table_query)
conn.commit()

cursor.close()
conn.close()


def save_to_db(tender_data):
    # Establish a connection to the PostgreSQL database

    if not check_lot_number_exists(lot_number=tender_data[8]):
        conn = psycopg2.connect(database="postgres", user="postgres",
                                password="postgres", host="localhost", port="5432")
        cursor = conn.cursor()
        # Insert the tender data into the "tenders" table
        insert_query = '''
        INSERT INTO tenders (
            lot_name,
            customer,
            lot_description,
            quantity,
            price,
            procurement_method,
            status,
            url_to_details,
            lot_number,
            start_date,
            end_date,
            customer_bin,
            customer_name,
            ktru_code,
            ktru_name,
            brief_characteristics,
            additional_characteristics,
            funding_source,
            unit_price,
            unit_measurement,
            year1_amount,
            year2_amount,
            year3_amount,
            planned_amount,
            advance_payment_percentage,
            delivery_location_kato,
            kato,
            tru_delivery_period,
            incoterms,
            lot_url,
            sended_to
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ''
        );
        '''

        cursor.execute(insert_query, tender_data)
        conn.commit()

        # Close the database connection
        cursor.close()
        conn.close()


# Нужные Настройки
user_agent = UserAgent()  # Создаем объект UserAgent
# отключение варнингов
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# решение ошибки связанной с кодировкой вывода в консоль
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# Нужные Настройки


def check_lot_number_exists(lot_number):
    conn = psycopg2.connect(database="postgres", user="postgres",
                            password="postgres", host="localhost", port="5432")

    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT EXISTS(SELECT 1 FROM tenders WHERE lot_number='{lot_number}')")
        result = cur.fetchone()[0]
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        file.write("\nError while executing SQL query:", error)
        return False
    finally:
        if conn is not None:
            conn.close()


def parse_all_tenders(url):
    '''Парсит все тендеры которые лежат в таблице на главной'''
    a = 0
    file.write('/nSTART2')
    headers = {
        'User-Agent': user_agent.random
    }
    all_tenders_html = requests.get(url, headers=headers, verify=False).text
    all_tenders_soup = BeautifulSoup(all_tenders_html, 'html.parser')
    try:
        # Находит таблицу -> tbody -> все горизонтальные записи
        tenders_rows = all_tenders_soup.find(
            "table", id="search-result").find('tbody').find_all("tr")
        if tenders_rows:
            for tender_row in tenders_rows:
                data_cells = tender_row.find_all('td')
                lot_number = str(data_cells[0].text.strip().split()[0])
                lot_name = str(data_cells[1].strong.text.strip())
                customer = str(
                    data_cells[1].small.text.strip().split("Заказчик: ")[-1])
                lot_description = str(data_cells[2].a.strong.text.strip())
                quantity = str(data_cells[3].text.strip())
                price = str(data_cells[4].strong.text.strip().replace(
                    ' ', '').replace(',', '.'))
                procurement_method = str(data_cells[5].text.strip())
                status = str(data_cells[6].text.strip())
                lot_id = lot_name.split()[0].split('-')[0]
                lot_number_raw = lot_number.split('-')[0]
                url_to_details = f"https://goszakup.gov.kz/ru/announce/index/{lot_id}?tab=lots#"
                file.write(f'\nТендер внешка ${a}')
                if not check_lot_number_exists(lot_number=lot_number):
                    while 1:
                        try:
                            parse_detail_tender(lot_id, lot_name, customer, lot_description, quantity,
                                        price, procurement_method, status, url_to_details,) # ID объявления

                            file.write(f"/n{lot_number} - Успешно БД СОХРАНИЛОСЬ")
                            break
                        except:
                            file.write(f"/nТендер внутрь - Fail {lot_number}")
                            time.sleep(3)

                else:
                    file.write(f"/n{a} - Запись {lot_number} уже существует - Не добавленно в БД")

                a += 1
            return
        else:
            file.write(f'\nerrror captcha')

    except:
        file.write(f'\nerrror captcha')
        return


def parse_detail_tender(post_id, lot_name, customer, lot_description, quantity,
                                price, procurement_method, status, url_to_details):
    a =  f'https://goszakup.gov.kz/ru/announce/index/{post_id}?tab=lots#'
    response = requests.get(
        f'https://goszakup.gov.kz/ru/announce/index/{post_id}?tab=lots#', verify=False).text
    soup = BeautifulSoup(response, 'html.parser')
    input_elements = soup.find_all('input', {'type': 'text', 'class': 'form-control'})
    start_date = input_elements[4].get('value')
    end_date = input_elements[5].get('value')
    table = soup.find('table', class_='table-bordered')
    data_lot_ids = []
    links = table.find_all('a', class_='btn-select-lot')
    for link in links:

        data_lot_id = link.get('data-lot-id')
        data_lot_ids.append([link.text.strip(), data_lot_id])
    for lot in data_lot_ids:
        data = {
            'id': lot[1],
        }
        response = requests.post(f'https://goszakup.gov.kz/ru/announce/ajax_load_lot/{post_id}?tab=lots', data=data, verify=False)
        if response.status_code == 200:
            lot_html = response.text
            lot_soup = BeautifulSoup(lot_html, 'html.parser')
            lot_table = lot_soup.find('table')

            if lot_table:
                lot_data = {}
                lot_rows = lot_table.find_all('tr')

                for lot_row in lot_rows:
                    header = lot_row.find('th').text.strip()
                    value = lot_row.find('td').text.strip()
                    lot_data[header] = value

                data = (
                    lot[0],
                    start_date,
                    end_date,
                    lot_data.get('БИН заказчика', 'Нет данных.'),
                    lot_data.get('Наименование заказчика', 'Нет данных.'),
                    lot_data.get('Код ТРУ', 'Нет данных.'),
                    lot_data.get('Наименование ТРУ', 'Нет данных.'),
                    lot_data.get('Краткая характеристика', 'Нет данных.'),
                    lot_data.get('Дополнительная характеристика', 'Нет данных.'),
                    lot_data.get('Источник финансирования', 'Нет данных.'),
                    lot_data.get('Цена за единицу', 'Нет данных.'),
                    lot_data.get('Единица измерения', 'Нет данных.'),
                    lot_data.get('Сумма 1 год', 'Нет данных.'),
                    lot_data.get('Сумма 2 год', 'Нет данных.'),
                    lot_data.get('Сумма 3 год', 'Нет данных.'),
                    lot_data.get('Запланированная сумма', 'Нет данных.'),
                    lot_data.get('Размер авансового платежа %', 'Нет данных.'),
                    lot_data.get('Место поставки товара, КАТО', 'Нет данных.'),
                    lot_data.get('Место поставки товара, КАТО', 'Нет данных.').split()[0].replace(',', ''),  # КАТО
                    lot_data.get('Срок поставки ТРУ', 'Нет данных.'),
                    lot_data.get('Условия поставки ИНКОТЕРМС', 'Нет данных.'),
                    f'https://goszakup.gov.kz/ru/announce/index/{post_id}?tab=lots#'
                )
                save_to_db((lot_name, customer, lot_description, quantity,
                                price, procurement_method, status, url_to_details, *data))
            else:
                file.write(f"/nNo table found for lot {lot[0]}")
        else:
            file.write(
                f"Failed to retrieve data for lot {lot[0]}. Status code: {response.status_code}")

if __name__ == "__main__":
    while 1:
        try:
            parse_all_tenders(
                'https://goszakup.gov.kz/ru/search/lots?count_record=100&page=1')
        except:
            print('error')
            time.sleep(10)
        time.sleep(10)
