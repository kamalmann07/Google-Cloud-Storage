from google.cloud import storage
import google.cloud.storage
import os
import json
import pandas as pd
from io import StringIO
import pickle

storage_client = google.cloud.storage.Client.from_service_account_json(service account key)

def readCSVFromCloud(bucket_name, blob_file_address, dest_file=''):
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(blob_file_address, bucket)
    # blob.download_to_filename(dest_file)
    str_cloud = blob.download_as_string()
    s = str(str_cloud, 'utf-8')
    data = StringIO(s)
    df = pd.read_csv(data)
    return df


def readJsonFromCloud(bucket_name, blob_file_address, dest_file=''):
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(blob_file_address, bucket)
    # blob.download_to_filename(dest_file)
    str_cloud = blob.download_as_string()
    my_json = str_cloud.decode('utf8').replace("'", '"')
    data = json.loads(my_json)
    s = json.dumps(data, indent=4)
    return s


def createJsonInCloud(bucket_name, blob_file_address, json_Data):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_file_address)
    blob.upload_from_string(json_Data)

def createCsvInCloud(bucket_name, blob_file_address, df):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_file_address)
    csv_data = df.to_csv(encoding="utf-8", index=False)
    blob.upload_from_string(csv_data)


def getFilesFromBlob(bucket_name, directory_prefix, file_format):
    file_directory = []
    bucket = storage_client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=directory_prefix):
        if file_format in blob.name:
            file_directory.append(blob.name)
            # print(blob.name)
    return file_directory

def moveFile(bucket_name, current_directory, updated_directory):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(current_directory)
    bucket.rename_blob(blob, new_name=updated_directory)



