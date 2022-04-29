import pandas as pd
import pandas_datareader as pdr
import os
from resources.gcloud.creds import *
from google.cloud import storage
from google.oauth2 import service_account


class File:
    extension = '.parquet.gzip'

    def __init__(self, ticker):
        self.ticker = ticker
        self.data_path = bucket_data_full_path + ticker + self.extension

    def exists(self):
        storage_credentials = service_account.Credentials.from_service_account_info(token)
        name = data_path + '/' + self.ticker + self.extension
        storage_client = storage.Client(credentials=storage_credentials)
        bucket = storage_client.bucket(bucket_name)
        return storage.Blob(bucket=bucket, name=name).exists(storage_client)

    def empty(self):
        return os.stat(self.data_path).st_size == 0

    def delete_file(self):
        storage_credentials = service_account.Credentials.from_service_account_info(token)
        storage_client = storage.Client(credentials=storage_credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(data_path + '/' + self.ticker + self.extension)
        blob.delete()

    def download(self, start, end):
        return pdr.get_data_yahoo(self.ticker, start=start, end=end).rename(
            columns={"High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME",
                     "Adj Close": "ADJ_CLOSE"})[["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

    def write(self, start, end):
        if self.exists():
            old_df = self.read()
            start = (old_df.index.max() + pd.DateOffset(1)).strftime("%Y-%m-%d")
            df = self.download(start, end)
            df = self.read().append(df)
            df.to_parquet(self.data_path, compression='gzip', storage_options={'token': token}, engine='pyarrow')
        else:
            df = self.download(start, end)
            df.to_parquet(self.data_path, compression='gzip', storage_options={'token': token}, engine='pyarrow')

    def read(self):
        if self.exists():
            return pd.read_parquet(self.data_path, storage_options={'token': token}, engine='pyarrow')
        else:
            raise FileNotFoundError("File does not exist on cloud storage. Please download before reading.")


f = File("AAPL")
df = f.read()

print(df)
