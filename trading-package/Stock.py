from dataclasses import dataclass
import pandas as pd
import pandas_datareader as pdr
import os
import sqlite3
from Data import Data


class Stock():
    def __init__(self, ticker, start, end=None):
        self.ticker = ticker
        self.start = start
        self.end = end

    def fetchData(self):
        self.data = pdr.get_data_yahoo('AAPL').reset_index(level=0).rename(
            columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME", "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]
