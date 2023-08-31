import sqlite3
import json

def save_data_to_sqlite(data_str, db_path):
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError:
        print("Failed to decode the JSON string.")
        return

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Handle list of dictionaries
    if isinstance(data, list) and data and isinstance(data[0], dict):
        headers = data[0].keys()
        columns = ', '.join([f'"{col}" TEXT' for col in headers])
        table_name = "dict_data"
        sql_create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(sql_create_table_command)
        for item in data:
            placeholders = ', '.join(['?'] * len(item.values()))
            sql_insert_command = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(sql_insert_command, tuple(item.values()))
    
    # Handle list of integers (or other atomic data types)
    elif isinstance(data, list) and data and isinstance(data[0], (int, float, str)):
        table_name = "atomic_data"
        sql_create_table_command = f"CREATE TABLE IF NOT EXISTS {table_name} (value)"
        cursor.execute(sql_create_table_command)
        for value in data:
            sql_insert_command = f"INSERT INTO {table_name} VALUES (?)"
            cursor.execute(sql_insert_command, (value,))
    
    else:
        print("Unsupported JSON format.")
        conn.close()
        return

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
