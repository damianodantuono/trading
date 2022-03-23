from Parameters import *
from trading import *
import dotenv

dotenv.load_dotenv()

aapl = Stock('AAPL', start='2015-01-01')

df = aapl.get_data()

df["HHV"] = add_donchian_channel_up(df.HIGH, number_of_periods)
df["LLV"] = add_donchian_channel_down(df.LOW, number_of_periods)
df["STD"] = add_standard_deviation(df["ADJ_CLOSE"], number_of_periods)
df["mean"] = add_mean(df["ADJ_CLOSE"], number_of_periods)
df["ENTER"] = crossover(df["ADJ_CLOSE"], df.HHV.shift(1))
df["EXIT"] = crossunder(df["ADJ_CLOSE"], df["mean"] + number_of_standard_deviations * df["STD"])

dataframe = aapl.apply_trading_system(10000, 0.1, 0.01, Stock.LONG, 'limit', df.HHV.shift(1), df.ENTER, df.EXIT)
dataframe.to_csv("df.csv", sep=';', decimal=',')

rep = Report(dataframe, 'AAPL')
rep.performance_report()
