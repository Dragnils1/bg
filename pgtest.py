import pymysql

def remove_substring_from_column(host, username, password, database, substring):
    try:
        # Connect to the database
        connection = pymysql.connect(host=host, user=username, password=password, db=database)

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Define the S дэдфышоритждлвфыатжифывжлапэджлфиувазпроиэлджфытэдвлитадлтждьж_+_+_+__+=\\L query to update the column
        update_query = f"""
            UPDATE filters
            SET telegram_chat_ids = REPLACE(telegram_chat_ids, %s, '')
            WHERE telegram_chat_ids LIKE %s
        """

        # Execute the update query with dynamic values
        cursor.execute(update_query, (f',{substring}', f'%{substring}%'))

        # Commit the changes to the database
        connection.commit()

        print("Update successful!")
        
    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Usage example
if __name__ == "__main__":
    host = "localhost"
    username = "postgres"
    password = "12345678"
    database = "postgres"
    substring_to_remove = "444"

    remove_substring_from_column(host, username, password, database, substring_to_remove)
