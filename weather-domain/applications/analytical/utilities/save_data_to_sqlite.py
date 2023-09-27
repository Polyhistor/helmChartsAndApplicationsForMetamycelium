import sqlite3
import csv
from io import StringIO

def save_data_to_sqlite(data_str, db_path):
    # Convert string data into a file-like object for csv reader
    csv_file = StringIO(data_str)
    reader = csv.reader(csv_file)
    
    # Extract headers (column names) from the first row
    headers = next(reader)

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    columns = ', '.join([f'"{col}" TEXT' for col in headers])
    table_name = "weather_data"
    sql_create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    cursor.execute(sql_create_table_command)

    # Insert rows from csv data into the table
    for row in reader:
        placeholders = ', '.join(['?'] * len(row))
        sql_insert_command = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql_insert_command, row)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
