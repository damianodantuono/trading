from trading_package import Stock
import dotenv

dotenv.load_dotenv()

aapl = Stock('AAPL', start='2015-01-01')

df = aapl.add_donchian_channel(20)

df["enterRule"] = Stock.crossover(df.CLOSE, df.HHV20.shift(1))
df["exitRule"] = Stock.crossunder(df.CLOSE, df.LLV20.shift(1))

dataframe = aapl.apply_trading_system(50000, 0.1, 0.01, Stock.LONG, 'limit', df.HHV20.shift(1), df.enterRule,
                                      df.exitRule)

dataframe.to_csv("test.csv", sep=';', decimal=',')
