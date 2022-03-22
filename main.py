import trading_package
import os
import dotenv

dotenv.load_dotenv()

aapl = trading_package.Stock('AAPL', start='2015-01-01')

df = aapl.addDonchianChannel(20)

df["enterRule"] = trading_package.Stock.crossover(df.CLOSE, df.HHV20.shift(1))
df["exitRule"] = trading_package.Stock.crossunder(df.CLOSE, df.LLV20.shift(1))

dataframe = aapl.applyTradingSystem(50000, 0.1, 20, 0.01, 'long', 'limit', df.HHV20.shift(1), df.enterRule, df.exitRule)

dataframe.to_csv("test.csv", sep=';', decimal=',')
