import sqlite3

def insert_into_db(storage_info):
    conn = sqlite3.connect('app_data.db')
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO storage_info (distributedStorageAddress, minio_access_key, minio_secret_key, bucket_name, object_name)
    VALUES (?, ?, ?, ?, ?)
    """, (
        storage_info["distributedStorageAddress"],
        storage_info["minio_access_key"],
        storage_info["minio_secret_key"],
        storage_info["bucket_name"],
        storage_info["object_name"]
    ))
    conn.commit()
    conn.close()