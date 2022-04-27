import pandas as pd
import pandas_datareader as pdr
import os


class File:
    extension = '.parquet.gzip'

    def __init__(self, path, ticker):
        self.ticker = ticker
        self.path = path
        self.data_path = path + os.sep + ticker + self.extension

    def exists(self):
        return os.path.exists(self.data_path)

    def empty(self):
        return os.stat(self.data_path).st_size == 0

    def download(self):
        return pdr.get_data_yahoo(self.ticker).reset_index(level=0).rename(
            columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME",
                     "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

    def write(self):
        df = self.download()
        df.to_parquet(self.data_path, compression='gzip')

    def read(self):
        return pd.read_parquet(self.data_path)
