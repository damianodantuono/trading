import pandas as pd
import os
from trading.Stock import Stock
import parameters

tickers = pd.read_html(parameters.SP500)[0]["Symbol"].values.tolist()

for i, ticker in enumerate(tickers):
    complete = round(i/len(tickers) * 100, 2)
    print("Downloading", ticker, complete, "%")
    try:
        stock = Stock(ticker, start=parameters.start).update_data()
    except Exception as ex:
        print(ex)
