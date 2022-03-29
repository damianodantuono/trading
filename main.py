import dotenv
import os
import pandas as pd

from parameters import *
from trading import *


def main():
    dotenv.load_dotenv()

    directory = os.listdir(".")

    for item in directory:
        if item.endswith(".csv"):
            os.remove(os.path.join(".", item))

    tickers = pd.read_html(os.getenv("SP500"))[0]["Symbol"].values.tolist()
    reports = []
    for ticker in tickers[:2]:
        stock = Stock(ticker, start='2010-01-01')
        df = stock.get_data()
        df["HHV"] = add_donchian_channel_up(df.HIGH, number_of_periods)
        df["LLV"] = add_donchian_channel_down(df.LOW, number_of_periods)
        df["STD"] = add_standard_deviation(df["ADJ_CLOSE"], number_of_periods)
        df["MEAN"] = add_mean(df["ADJ_CLOSE"], number_of_periods)
        df["ENTER"] = crossover(df["HIGH"], df.HHV.shift(1))
        df["EXIT"] = crossunder(df["ADJ_CLOSE"], df["MEAN"])

        #df.to_csv("dfr.csv", sep=";", float_format="%.3f")

        dataframe = stock.apply_trading_system(
            money, fees, tick, Stock.LONG, 'limit', df.HHV.shift(1), df.ENTER, df.EXIT)
        reports.append(Report(dataframe, ticker).table_report())
    dataframe = pd.concat(reports)
    dataframe.to_csv("dfr.csv", sep=";", float_format="%.3f")


if __name__ == '__main__':
    main()
