import sqlite3
import json

def save_data_to_sqlite(data_str):
    conn = sqlite3.connect('customer_data.db')
    cursor = conn.cursor()
    
    lines = data_str.strip().split('\n')
    data_json = [json.loads(line) for line in lines]
    
    # Assume all items in the JSON have the same structure.
    # We use the first item to determine the columns
    if not data_json:
        print("No data to save!")
        return

    first_item = data_json[0]
    columns = ', '.join([f'"{col}" TEXT' for col in first_item.keys()])

    cursor.execute(f"CREATE TABLE IF NOT EXISTS customer_data ({columns})")

    for item in data_json:
        keys = ', '.join([f'"{k}"' for k in item.keys()])
        question_marks = ', '.join(['?' for _ in item.values()])
        cursor.execute(f"INSERT INTO customer_data ({keys}) VALUES ({question_marks})", list(item.values()))

    conn.commit()
    conn.close()