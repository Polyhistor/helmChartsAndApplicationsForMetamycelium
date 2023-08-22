import sqlite3
import json

def save_data_to_sqlite(data_str):
    conn = sqlite3.connect('customer_data.db')
    cursor = conn.cursor()
    
    lines = data_str.strip().split('\n')
    data_json = [json.loads(line) for line in lines]
    
    # If there's no data to save, return early
    if not data_json:
        print("No data to save!")
        return

    first_item = data_json[0]

    # Ensure table exists
    columns = ', '.join([f'"{col}" TEXT' for col in first_item.keys()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS customer_data ({columns})")

    # Check each item in data_json
    for item in data_json:
        keys = [f'"{k}"' for k in item.keys()]

        # Check if columns exist, if not, add them
        for k in keys:
            cursor.execute("PRAGMA table_info(customer_data)")
            columns_info = cursor.fetchall()
            column_names = [column[1] for column in columns_info]
            if k.replace('"', '') not in column_names:
                cursor.execute(f"ALTER TABLE customer_data ADD COLUMN {k} TEXT")

        question_marks = ', '.join(['?' for _ in item.values()])
        cursor.execute(f"INSERT INTO customer_data ({', '.join(keys)}) VALUES ({question_marks})", list(item.values()))

    conn.commit()
    conn.close()