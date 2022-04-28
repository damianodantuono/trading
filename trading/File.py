import pandas as pd
import pandas_datareader as pdr
import os
from resources.gcloud.creds import *
from google.cloud import storage
from google.oauth2 import service_account
import datetime


class File:
    extension = '.parquet.gzip'

    def __init__(self, ticker):
        self.ticker = ticker
        self.data_path = bucket_data_full_path + ticker + self.extension

    def exists(self):
        return os.path.exists(self.data_path) and not self.empty()

    def empty(self):
        return os.stat(self.data_path).st_size == 0

    def delete_file(self):
        storage_credentials = service_account.Credentials.from_service_account_info(token)
        storage_client = storage.Client(credentials=storage_credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("data/" + self.ticker + self.extension)
        blob.delete()

    def download(self, start, end):
        return pdr.get_data_yahoo(self.ticker, start=start, end=end).reset_index(level=0).rename(
            columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME",
                     "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

    def write(self, start, end):
        df = self.download(start, end)
        df.to_parquet(self.data_path, compression='gzip', storage_options={'token': token})

    def read(self):
        return pd.read_parquet(self.data_path, storage_options={'token': token})

