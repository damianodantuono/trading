import os
from Parameters import *
from trading import Stock, add_donchian_channel_down, add_donchian_channel_up, add_standard_deviation, crossunder, crossover, apply_function_to_series, add_mean
import dotenv
import numpy as np

dotenv.load_dotenv()

aapl = Stock('AAPL', start='2015-01-01')

df = aapl.get_data()
df["HHV"] = add_donchian_channel_up(df.HIGH, 20)
df["LLV"] = add_donchian_channel_down(df.LOW, 20)
df["STD"] = add_standard_deviation(df["ADJ_CLOSE"], 20)
df["mean"] = add_mean(df["ADJ_CLOSE"], 20)

df = df.dropna()

df["ENTER"] = crossover(df["ADJ_CLOSE"], df.HHV.shift(1))
df["EXIT"] = crossunder(df["ADJ_CLOSE"], df["mean"] + number_of_standard_deviations * df["STD"])

df.to_csv("test.csv", sep=';', decimal=',')
