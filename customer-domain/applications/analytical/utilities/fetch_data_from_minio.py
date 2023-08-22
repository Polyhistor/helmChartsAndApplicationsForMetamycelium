from minio import Minio

def fetch_data_from_minio(storage_info):

    minio_client = Minio(
        storage_info["distributedStorageAddress"],
        access_key=storage_info["minio_access_key"],
        secret_key=storage_info["minio_secret_key"],
        secure=False
    )

    # minio returns a stream of data 
    data = minio_client.get_object(storage_info["bucket_name"], storage_info["object_name"])
    data_str = ''

    # reading 32 kbs at a time, the code below is only for chunking and avoiding timeouts
    for d in data.stream(32*1024):
        data_str += d.decode()


    return data_str