import pandas_datareader as pdr
import os
from trading_package.Data import Data


class Stock():
    def __init__(self, ticker, start, end=None):
        self.ticker = ticker
        self.start = start
        self.end = end
        self.dataInterface = Data(os.getenv("DATA_DB_PATH"), ticker)

    def updateData(self, force=False):
        if not force:
            if not self.dataInterface.exists():
                self.dataInterface.createTable()
                self.dataInterface.insertData()
            elif self.dataInterface.empty():
                self.dataInterface.insertData()
            else:
                pass
        else:
            self.dataInterface.createTable()
            self.dataInterface.insertData()

    def clearData(self):
        self.dataInterface.dropTable()

    def add_donchian_channel(dataframe, values):
        """
        Add donchian channel for N windows.
        donchian channel: maximum of last N maxima and minimum of last N minima

        Args:
            dataframe (pandas.DataFrame): dataframe cointaining OHLC data.
        """
        values = set(values)

        tmp_dataframe = dataframe.copy()
        tmp_dataframe = tmp_dataframe.rename(columns=str.lower)
        for value in values:
            dataframe[f'hhv{value}'] = tmp_dataframe.high.rolling(value).max()
            dataframe[f'llv{value}'] = tmp_dataframe.low.rolling(value).min()

        dataframe.dropna(inplace=True)
        return dataframe
