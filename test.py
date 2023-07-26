import psycopg2
from datetime import datetime
import sys
import io
import schedule
import psycopg2
import time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def create_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()  
    
def update_sended_today():
    try:
        # Replace the connection parameters with your PostgreSQL database details
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="12345678"
        )

        cursor = connection.cursor()

        # Update the sended_today column to false for all records
        update_query = "UPDATE filters SET sended_today = false;"
        cursor.execute(update_query)

        connection.commit()
        cursor.close()
        connection.close()

        print("Successfully updated sended_today to false for all records.")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL or executing the update query:", error)

def main():
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="12345678"
        )

        # Выбор всех записей из таблицы filters
        select_filters_query = "SELECT * FROM filters"
        filters_data = execute_query(connection, select_filters_query)

        # Получаем текущее время
        current_time = datetime.now().strftime("%H:%M")

        for filter_data in filters_data:
            # Проверяем наличие айди чатов
            telegram_chat_ids = filter_data[8]  # Индекс 7 соответствует telegram_chat_ids
            if not telegram_chat_ids:
                continue

            if filter_data[9]: 
                print("уже отправлено")
                continue

            # Проверяем поле "во сколько времени отправлять"
            send_time = filter_data[7]  # Индекс 6 соответствует send_time
            if send_time:
                send_time_obj = datetime.strptime(send_time, "%H:%M")
                current_time_obj = datetime.strptime("23:59", "%H:%M")
                if current_time_obj < send_time_obj:
                    continue

            # Разбиваем строку с KTRU на список
            ktru_list = filter_data[3].split(",")  # Индекс 3 соответствует KTRU

            # Преобразуем start_price и end_price в целочисленные значения
            start_price = int(filter_data[5])  # Индекс 4 соответствует start_price
            end_price = int(filter_data[6])  # Индекс 5 соответствует end_price

            # Генерируем запросы и выполняем их
            for ktru in ktru_list:
                ktru = ktru.strip()
                kato = filter_data[4]  # Индекс 3 соответствует KATO

                query = f"SELECT * FROM tenders WHERE kato='{kato}' AND ktru_code='{ktru}' AND CAST(planned_amount as float)>={start_price} AND CAST(planned_amount as float)<={end_price}"
                
                print(query)
          
                results = execute_query(connection, query)
                for result in results:
                    print(result)
                    
            if send_time:
                query_update = f"UPDATE filters SET sended_today = true WHERE id = {filter_data[0]};"
                print(query_update)
                create_query(connection, query_update)

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL:", error) 

    finally:
        if connection:
            connection.close()
        
if __name__ == "__main__":
    # schedule.every().day.at("00:00").do(update_sended_today)
    main()
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)