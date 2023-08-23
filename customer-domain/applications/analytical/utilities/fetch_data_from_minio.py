from minio import Minio
import threading
minio_lock = threading.Lock()

def fetch_data_from_minio(storage_info):

    with minio_lock:
        minio_client = Minio(
            storage_info["distributedStorageAddress"],
            access_key=storage_info["minio_access_key"],
            secret_key=storage_info["minio_secret_key"],
            secure=False
        )

        # minio returns a stream of data 
        data = minio_client.get_object(storage_info["bucket_name"], storage_info["object_name"])
        
        chunks = []

        # reading 32 kbs at a time, the code below is only for chunking and avoiding timeouts
        for d in data.stream(32*1024):
            chunks.append(d)
        
        # Concatenate all the chunks and then decode the entire data once
        data_str = b''.join(chunks).decode('utf-8')

        return data_str
