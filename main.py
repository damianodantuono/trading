import trading_package
import pandas_datareader as pdr
import os
import dotenv

dotenv.load_dotenv()

df = pdr.get_data_yahoo('AAPL').reset_index(level=0).rename(
    columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME", "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

trading_package.Data.createTable(os.getenv("DATA_DB_PATH"), 'TSLA')

print(trading_package.Data.empty(os.getenv("DATA_DB_PATH"), 'TSLA'))
