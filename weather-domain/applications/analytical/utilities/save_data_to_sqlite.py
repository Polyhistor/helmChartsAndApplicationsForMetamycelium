import sqlite3
from io import StringIO
import csv

def save_data_to_sqlite(data_str, table_to_save_data_to):
    conn = sqlite3.connect(table_to_save_data_to)
    cursor = conn.cursor()
    data_file = StringIO(data_str)
    reader = csv.reader(data_file)
    header = next(reader)
    columns = ', '.join([f'{col} TEXT' for col in header])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS weather_data ({columns})")
    for row in reader:
        cursor.execute(f"INSERT INTO weather_data VALUES ({', '.join(['?' for _ in row])})", row)
    conn.commit()
    conn.close()