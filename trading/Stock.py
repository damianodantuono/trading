import pandas as pd
import numpy as np
import os
import math
from trading.Data import Data
from collections import deque


class Stock:
    LONG = True
    SHORT = False

    def __init__(self, ticker, start, end=None):
        self.ticker = ticker
        self.start = start
        self.end = end
        self.dataInterface = Data(os.getenv("DATA_DB_PATH"), ticker)

    def update_data(self, force=False):
        if not force:
            if not self.dataInterface.exists():
                self.dataInterface.createTable()
                self.dataInterface.insertData()
            elif self.dataInterface.empty():
                self.dataInterface.insertData()
            else:
                pass
        else:
            self.dataInterface.createTable()
            self.dataInterface.insertData()

    def clear_data(self):
        self.dataInterface.dropTable()

    def get_data(self):
        return self.dataInterface.loadDataframe()

    @staticmethod
    def limit_check(dataframe, rules, level, direction):
        service_dataframe = pd.DataFrame(index=dataframe.index)
        service_dataframe['rules'] = rules
        service_dataframe['level'] = level
        service_dataframe['low'] = dataframe.low
        service_dataframe['high'] = dataframe.high

        if direction:
            service_dataframe['new_rules'] = np.where(service_dataframe.rules & (service_dataframe.low.shift(-1) <= service_dataframe.level.shift(-1)), True, False)

        else:
            service_dataframe['new_rules'] = np.where(service_dataframe.rules & (service_dataframe.high.shift(-1) >= service_dataframe.level.shift(-1)), True, False)

        return service_dataframe.new_rules

    @staticmethod
    def tick_correction_down(level, tick):
        if level != level:
            level = 0
        multipier = math.floor(level / tick)
        return multipier * tick

    @staticmethod
    def tick_correction_up(level, tick):
        if level != level:
            level = 0
        multipier = math.ceil(level / tick)
        return multipier * tick

    @staticmethod
    def market_position_generator(enter_rule, exit_rule):
        status = 0
        market_positions = []
        for i, j in zip(enter_rule, exit_rule):
            if status == 0:
                if i and not j:
                    status = 1
            else:
                if j:
                    status = 0
            market_positions.append(status)
        market_positions = deque(market_positions)
        market_positions.rotate(1)
        market_positions[0] = 0
        return list(market_positions)

    def apply_trading_system(self, money: float, fees: float, tick: float, direction: bool, order_type: str, enter_level: pd.Series, entry_rules: pd.Series, exit_rules: pd.Series) -> pd.DataFrame:

        dataframe = self.dataInterface.loadDataframe()
        dataframe = dataframe.rename(columns=str.lower)
        if order_type == 'limit':
            entry_rules = self.limit_check(
                dataframe, entry_rules, enter_level, direction)

        dataframe['enter_level'] = enter_level
        dataframe['enter_rules'] = entry_rules
        dataframe['exit_rules'] = exit_rules
        dataframe['market_position'] = self.market_position_generator(entry_rules, exit_rules)

        if order_type == 'limit':
            if direction:
                dataframe.enter_level = dataframe.enter_level.apply(lambda x: self.tick_correction_down(x, tick))
                real_entry = np.where(dataframe.open < dataframe.enter_level, dataframe.open, dataframe.enter_level)
                dataframe["entry_price"] = np.where((dataframe.market_position.shift(1) == 0) & (dataframe.market_position == 1), real_entry, np.nan)
            else:
                dataframe.enter_level = dataframe.enter_level.apply(lambda x: self.tick_correction_up(x, tick))
                real_entry = np.where(dataframe.open > dataframe.enter_level, dataframe.open, dataframe.enter_level)
                dataframe["entry_price"] = np.where((dataframe.market_position.shift(1) == 0) & (dataframe.market_position == 1), real_entry, np.nan)

            dataframe["number_of_stocks"] = np.where((dataframe.market_position.shift(1) == 0) & (dataframe.market_position == 1), money / real_entry, np.nan)

        dataframe["entry_price"] = dataframe["entry_price"].fillna(method='ffill')
        dataframe["events_in"] = np.where((dataframe.market_position == 1) & (dataframe.market_position.shift(1) == 0), "entry", "")

        dataframe["number_of_stocks"] = dataframe["number_of_stocks"].apply(lambda x: round(x, 0)).fillna(method='ffill')

        if direction:
            dataframe["open_operations"] = (dataframe.close - dataframe.entry_price) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where(
                (dataframe.market_position == 1) & (dataframe.market_position.shift(-1) == 0),
                (dataframe.open.shift(-1) -
                 dataframe.entry_price)
                * dataframe.number_of_stocks - 2 * fees,
                dataframe.open_operations)

        else:
            dataframe["open_operations"] = (dataframe.entry_price - dataframe.close) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where(
                (dataframe.market_position == 1) & (dataframe.market_position.shift(-1) == 0),
                (dataframe.entry_price -
                 dataframe.open.shift(-1))
                * dataframe.number_of_stocks - 2 * fees,
                dataframe.open_operations)

        dataframe["open_operations"] = np.where(
            dataframe.market_position == 1, dataframe.open_operations, 0)
        dataframe["events_out"] = np.where(
            (dataframe.market_position == 1) & dataframe.exit_rules, "exit", "")
        dataframe["operations"] = np.where(dataframe.exit_rules & (dataframe.market_position == 1),
                                           dataframe.open_operations, np.nan)
        dataframe["closed_equity"] = dataframe.operations.fillna(0).cumsum()
        dataframe["open_equity"] = dataframe.closed_equity + dataframe.open_operations - dataframe.operations.fillna(0)

        number_initial_stocks = money / dataframe.close[0]

        dataframe["B&H"] = number_initial_stocks * (dataframe.close - dataframe.close[0])

        return dataframe
