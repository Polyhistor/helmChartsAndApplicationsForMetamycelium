from minio import Minio
import io

minio_url = "localhost:9001"
minio_acces_key = "minioadmin"
minio_secret_key = "minioadmin"

minioClient = Minio(minio_url,
                    access_key=minio_acces_key,
                    secret_key=minio_secret_key,
                    secure=False)

def upload_data_to_minio(bucket_name, data):
    # Ensure bucket exists or create
    if not minioClient.bucket_exists(bucket_name):
        minioClient.make_bucket(bucket_name)

    # Upload the data
    data_bytes = data.encode('utf-8')
    file_size = len(data_bytes)
    minioClient.put_object(bucket_name, 'data.json', io.BytesIO(data_bytes), file_size, content_type='application/json')
