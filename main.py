import dotenv
import os
import pandas as pd

from Parameters import *
from trading import *


def main():
    dotenv.load_dotenv()

    tickers = pd.read_html(os.getenv("SP500"))[0]["Symbol"].values.tolist()

    for ticker in tickers:
        stock = Stock(ticker, start='2010-01-01')
        df = stock.get_data()
        df["HHV"] = add_donchian_channel_up(df.HIGH, number_of_periods)
        df["LLV"] = add_donchian_channel_down(df.LOW, number_of_periods)
        df["STD"] = add_donchian_channel_up(df["ADJ_CLOSE"], number_of_periods)
        df["MEAN"] = add_donchian_channel_up(df["ADJ_CLOSE"], number_of_periods)
        df["ENTER"] = crossover(df["ADJ_CLOSE"], df.HHV.shift(1))
        df["EXIT"] = crossunder(df["ADJ_CLOSE"], df["MEAN"] + number_of_standard_deviations * df["STD"])

        dataframe = stock.apply_trading_system(money, fees, tick, Stock.LONG, 'limit', df.HHV.shift(1), df.ENTER, df.EXIT)
