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

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(database="postgres", user="postgres",
                        password="postgres", host="localhost", port="5432")
cursor = conn.cursor()

# Create the "tenders" table
create_table_query = '''
CREATE TABLE IF NOT EXISTS tenders (
    lot_number TEXT,
    kato TEXT,
    lot_name TEXT,
    customer TEXT,
    lot_description TEXT,
    quantity TEXT,
    price TEXT,
    procurement_method TEXT,
    status TEXT,
    url_to_details TEXT,
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
    tru_delivery_period TEXT,
    incoterms TEXT,
    dumping_sign TEXT
);
'''
cursor.execute(create_table_query)
conn.commit()

cursor.close()
conn.close()


def save_to_db(tender_data):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(database="postgres", user="postgres",
                            password="postgres", host="localhost", port="5432")
    cursor = conn.cursor()

    # Insert the tender data into the "tenders" table
    insert_query = '''
    INSERT INTO tenders (
        lot_number,
        kato,
        lot_name,
        customer,
        lot_description,
        quantity,
        price,
        procurement_method,
        status,
        url_to_details,
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
        tru_delivery_period,
        incoterms,
        dumping_sign
    )
    VALUES (
        %s, %s %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    '''

    cursor.execute(insert_query, tender_data)
    conn.commit()

    # Close the database connection
    cursor.close()
    conn.close()

### Нужные Настройки
user_agent = UserAgent() # Создаем объект UserAgent
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # отключение варнингов
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8') # решение ошибки связанной с кодировкой вывода в консоль
### Нужные Настройки

def check_lot_number_exists(lot_number):
    conn = psycopg2.connect(database="postgres", user="postgres", password="12345678", host="localhost", port="5432")

    try:
        cur = conn.cursor()
        cur.execute("SELECT EXISTS(SELECT 1 FROM tenders WHERE lot_number = %s)", (lot_number,))
        result = cur.fetchone()[0]
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while executing SQL query:", error)
        return False
    finally:
        if conn is not None:
            conn.close()

def parse_all_tenders(urls_with_filters: list, kato):

    '''ВАЖНО: принимает СПИCОК урлов, т.к. хз каждый раз сколько КТРУ попросят
    Парсит все тендеры которые лежат в таблице на главной,
    в этом случае все тендеры которые подходят по фильтрам
    т.е. которые возвращает сам сайт по юрл из аргумента с забитыми фильтрами
    пример страницы -> https://goszakup.gov.kz/ru/search/lots?filter%5Benstru%5D=711220.000.000000&filter%5Bkato%5D=590000000&filter%5Bamount_from%5D=500000&count_record=100&page=1'''

    a=0
    print('START2')
    for url in urls_with_filters:
        print('START3')

        headers = {
        'User-Agent': user_agent.random
        }
        all_tenders_html = requests.get(url, headers=headers, verify=False).text
        all_tenders_soup = BeautifulSoup(all_tenders_html, 'html.parser')
        tenders_rows = all_tenders_soup.find("table", id="search-result").find('tbody').find_all("tr") #Находит таблицу -> tbody -> все горизонтальные записи
        for tender_row in tenders_rows:
            data_cells = tender_row.find_all('td')
            lot_number = str(data_cells[0].text.strip().split()[0])
            lot_name = str(data_cells[1].strong.text.strip())
            customer = str(data_cells[1].small.text.strip().split("Заказчик: ")[-1])
            lot_description = str(data_cells[2].a.strong.text.strip())
            quantity = str(data_cells[3].text.strip())
            price = str(data_cells[4].strong.text.strip().replace(' ', '').replace(',', '.'))
            procurement_method = str(data_cells[5].text.strip())
            status = str(data_cells[6].text.strip())
            url_to_details = data_cells[2].a['href']
            # print("lot_number:", lot_number)
            # print("lot_name:", lot_name)
            # print("customer:", customer)
            # print("lot_description:", lot_description)
            # print("quantity:", quantity)
            # print("price:", price)
            # print("procurement_method:", procurement_method)
            # print("status:", status)
            # print("url_to_details:", url_to_details)
            print(f'Тендер внешка ${a}')
            if not check_lot_number_exists(lot_number=lot_number):
                while 1:
                    try:
                        detail_data = parse_detail_tender(url_to_details=url_to_details)
                        print(f'Тендер внутрь ${a} + Success')
                        break
                    except:
                        print(f"Тендер внутрь - Fail {a}")
                        time.sleep(5)
                save_to_db((lot_number, kato, lot_name, customer, lot_description, quantity, price, procurement_method, status, url_to_details, *detail_data))
            else:
                print(f"{a} - Запись {lot_number} уже существует - Не добавленно в БД")
            a+=1
        print(a)
    return

def parse_detail_tender(url_to_details: str):

    '''Парсит внутренние данные по каждому тендеру,
    типа детальной странице с дополнительными данными
    пример -> https://goszakup.gov.kz/ru/subpriceoffer/index/10363605/26850673'''
    # Генерируем новый User-Agent при каждом запросе
    headers = {
        'User-Agent': user_agent.random
    }

    # Отправляем GET-запрос с новым User-Agent
    response = requests.get(url_to_details, headers=headers, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table')
    rows = table.find_all('tr')

    item = {}
    for row in rows:
        th = row.find('th')
        td = row.find('td')
        if th and td:
            value = td.text.strip() if td.text else "Нет данных."
            item[th.text.strip()] = value

    return (

        item.get('Дата начала приема заявок', '') or "Нет данных.",
        item.get('Дата окончания приема заявок', '') or "Нет данных.",
        item.get('БИН заказчика', '') or "Нет данных.",
        item.get('Наименование заказчика', '') or "Нет данных.",
        item.get('Код ТРУ', '') or "Нет данных.",
        item.get('Наименование ТРУ', '') or "Нет данных.",
        item.get('Краткая характеристика', '') or "Нет данных.",
        item.get('Дополнительная характеристика', '') or "Нет данных.",
        item.get('Источник финансирования', '') or "Нет данных.",
        item.get('Цена за единицу', '') or "Нет данных.",
        item.get('Единица измерения', '') or "Нет данных.",
        item.get('Сумма 1 год', '') or "Нет данных.",
        item.get('Сумма 2 год', '') or "Нет данных.",
        item.get('Сумма 3 год', '') or "Нет данных.",
        item.get('Запланированная сумма', '') or "Нет данных.",
        item.get('Размер авансового платежа %', '') or "Нет данных.",
        item.get('Место поставки товара, КАТО', '') or "Нет данных.",
        item.get('Срок поставки ТРУ', '') or "Нет данных.",
        item.get('Условия поставки ИНКОТЕРМС', '') or "Нет данных.",
        item.get('Признак демпинга', '') or "Нет данных."
    )

def create_urls_with_filters(ktru_list: list, price_start_from: str, kato: str):

    '''Генерирует список урлов с готовыми фильтрами для parse_all_tenders()
    берет КТРУ из списка КТРУ и делает ссылку с каждым КТРУ, Ценой от, регионом'''

    urls_with_filters = []

    for ktru in ktru_list:
        urls_with_filters.append(f"https://goszakup.gov.kz/ru/search/lots?filter%5Benstru%5D={ktru}&filter%5Bkato%5D={kato}&filter%5Bamount_from%5D={price_start_from}&count_record=100&page=1")

    return urls_with_filters


# ktru_list = ['711220.000.000000', '139229.800.000002']
# print(create_urls_with_filters(ktru_list=ktru_list, price_start_from=500000, kato=590000000))



if __name__ == "__main__":
    ktru_list = ['711220.000.000000']
    parse_all_tenders(create_urls_with_filters(ktru_list=ktru_list, price_start_from=500000, kato=590000000), kato=590000000)
    print('START1')
    # Close the database connection
