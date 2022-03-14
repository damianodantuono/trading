from cgi import print_arguments
import trading
import pandas_datareader as pdr

df = pdr.get_data_yahoo('AAPL').reset_index(level=0).rename(
    columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME", "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

trading.Data.insertData('data.db', 'AAPL', df)
