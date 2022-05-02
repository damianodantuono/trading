import os

import numpy as np
import pandas as pd
import parameters
from trading import *


def main():
    tickers = pd.read_html(parameters.SP500)[0]["Symbol"].values.tolist()
    reports = []
    for ticker in tickers[:1]:
        stock = Stock(ticker, start=parameters.start)
        df = stock.get_data()
        df["ATR"] = add_average_true_range(df.HIGH - df.LOW, np.abs(df.HIGH - df.CLOSE.shift()), np.abs(df.LOW - df.CLOSE.shift()), parameters.number_of_periods)
        print(df)
        """
        df["HHV"] = add_donchian_channel_up(df.HIGH, parameters.number_of_periods)
        df["LLV"] = add_donchian_channel_down(df.LOW, parameters.number_of_periods)
        df["STD"] = add_standard_deviation(df["ADJ_CLOSE"], parameters.number_of_periods)
        df["MEAN"] = add_mean(df["ADJ_CLOSE"], parameters.number_of_periods)
        df["ENTER"] = crossover(df["HIGH"], df.HHV.shift(1))
        df["EXIT"] = crossunder(df["ADJ_CLOSE"], df["MEAN"])

        #df.to_csv("dfr.csv", sep=";", float_format="%.3f")

        dataframe = stock.apply_trading_system(
            parameters.money, parameters.fees, parameters.tick, Stock.LONG, 'limit', df.HHV.shift(1), df.ENTER, df.EXIT)
        reports.append(Report(dataframe, ticker).table_report())
    dataframe = pd.concat(reports)
    dataframe.to_csv("dfr.csv", sep=";", float_format="%.3f")
    """


if __name__ == '__main__':
    main()
