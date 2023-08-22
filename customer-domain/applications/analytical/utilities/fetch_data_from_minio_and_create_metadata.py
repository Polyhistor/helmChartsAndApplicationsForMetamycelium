from datetime import datetime
import time 
from utilities import fetch_data_from_minio, save_data_to_sqlite, create_metadata, get_all_storage_from_db
from utilities import save_data_to_sqlite

def fetch_data_from_minio_and_create_metadata():
    # Determine the actual time as the current timestamp
    actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    # Fetch all the storage information from the SQLite database
    all_storage_info = get_all_storage_from_db.get_all_storage_from_db()

    print(all_storage_info)

    for storage_info in all_storage_info:
        data_str = fetch_data_from_minio.fetch_data_from_minio(storage_info)
        save_data_to_sqlite.save_data_to_sqlite(data_str)

    processing_duration = time.time() - start_time
    metadata = create_metadata.create_metadata(actual_time, processing_duration, data_str)

    return metadata