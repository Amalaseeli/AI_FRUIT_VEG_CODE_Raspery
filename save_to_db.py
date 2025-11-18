from db_utils import DatabaseConnector
import pyodbc

db = DatabaseConnector()

def save_detected_product(json_txt):
    connection = db.create_connection()
    if connection is None:
        print("Warning: Database connection not established. Skipping save.")
        return
    try:
        cursor = None
        cursor = connection.cursor()

        # Check if table exists
        cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'AITransaction'
        """)

        # If the table doesnot exists
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            CREATE TABLE AITransaction (
                AIJsonTxt NVARCHAR(MAX) NOT NULL
            )
            """)
            # Immediately seed with current payload so a row always exists
            cursor.execute("INSERT INTO AITransaction (AIJsonTxt) VALUES (?)", (json_txt,))
            connection.commit()
        else:
            # Ensure at least one row exists; insert if table is empty
            cursor.execute("SELECT COUNT(*) FROM AITransaction")
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                cursor.execute("INSERT INTO AITransaction (AIJsonTxt) VALUES (?)", (json_txt,))
            else:
                # Update a single row to hold the latest payload
                cursor.execute("UPDATE TOP (1) AITransaction SET AIJsonTxt = ?", (json_txt,))
            connection.commit()
    except pyodbc.Error as e:
        print(f"Error: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()



def clear_database():
    connection = db.create_connection()
    if connection is None:
        return
    try:
        save_detected_product('[]')
    except pyodbc.Error as e:
        print(f"Error: {e}")
    finally:
        connection.close()
    
