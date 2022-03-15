import trading_package
import pandas_datareader as pdr
import os
import dotenv

dotenv.load_dotenv()

trading_package.Stock('AAPL', '2021-01-01').clearData()
trading_package.Stock('AAPL', '2021-01-01').updateData()
