from datetime import datetime
import time 
from utilities import fetch_data_from_minio
from utilities import save_data_to_sqlite


def fetch_data_from_minio_and_save():
    # Determine the actual time as the current timestamp
    actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()
    data_str = fetch_data_from_minio()
    save_data_to_sqlite(data_str)
    processing_duration = time.time() - start_time

    metadata = create_metadata(actual_time, processing_duration, data_str)
    print(metadata)

    return metadata