import pandas as pd
import os
from trading.Stock import Stock
import parameters
import dotenv
import time

dotenv.load_dotenv()

tickers = pd.read_html(os.getenv("SP500"))[0]["Symbol"].values.tolist()

for i, ticker in enumerate(tickers[:1]):
    ticker = "AAPL"
    complete = round(i/len(tickers) * 100, 2)
    print("Downloading", ticker, complete, "%")
    try:
        stock = Stock(ticker, start=parameters.start).update_data(force=True)
    except Exception as ex:
        print(ex)
