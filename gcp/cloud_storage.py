
import io
import os
from google.cloud import storage


bucket_name = os.environ.get("BUCKET_NAME", "oct-bucket-team6")
## os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ='credentials.json'
storage_client =storage.Client()

BLOB_FORMAT = "{}/{}/{}/{}.{}"


def upload_to_bucket(blob_name,file_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_name)
        return True
    except Exception as e:
        print(e)
        return False


def download_file_from_bucket(blob_name,file_path):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with open(file_path, 'wb') as archivo:
            storage_client.download_blob_to_file(blob,archivo)
        
        return True
    except Exception as e:
        print(e)
        return False
