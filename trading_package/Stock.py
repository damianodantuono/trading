import pandas as pd
import numpy as np
import os
import math
from trading_package.Data import Data
from collections import deque


class Stock():
    def __init__(self, ticker, start, end=None):
        self.ticker = ticker
        self.start = start
        self.end = end
        self.dataInterface = Data(os.getenv("DATA_DB_PATH"), ticker)

    def updateData(self, force=False):
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

    def clearData(self):
        self.dataInterface.dropTable()

    def getData(self):
        return self.dataInterface.loadDataframe()

    def addDonchianChannel(self, periods):
        df = self.dataInterface.loadDataframe()
        df[f'HHV{periods}'] = df.HIGH.rolling(periods).max()
        df[f'LLV{periods}'] = df.LOW.rolling(periods).min()

        return df.dropna()

    def limitCheck(self, dataframe, rules, level, direction):
        service_dataframe = pd.DataFrame(index=dataframe.index)
        service_dataframe['rules'] = rules
        service_dataframe['level'] = level
        service_dataframe['low'] = dataframe.low
        service_dataframe['high'] = dataframe.high

        if direction == 'long':
            service_dataframe['new_rules'] = np.where((service_dataframe.rules == True) & (
                service_dataframe.low.shift(-1) <= service_dataframe.level.shift(-1)), True, False)

        if direction == 'short':
            service_dataframe['new_rules'] = np.where((service_dataframe.rules == True) & (
                service_dataframe.high.shift(-1) >= service_dataframe.level.shift(-1)), True, False)

        return service_dataframe.new_rules

    def tickCorrectionDown(self, level, tick):
        if level != level:
            level = 0
        multipier = math.floor(level/tick)
        return multipier * tick

    def tickCorrectionUp(self, level, tick):
        if level != level:
            level = 0
        multipier = math.ceil(level/tick)
        return multipier * tick

    def marketPositionGenerator(self, enterRule, exitRule):
        status = 0
        marketPositions = []
        for i, j in zip(enterRule, exitRule):
            if status == 0:
                if i and not j:
                    status = 1
            else:
                if j:
                    status = 0
            marketPositions.append(status)
        marketPositions = deque(marketPositions)
        marketPositions.rotate(1)
        marketPositions[0] = 0
        return list(marketPositions)

    def applyTradingSystem(self, money, fees, periods, tick, direction, orderType, enterLevel, entryRules, exitRules):
        dataframe = self.dataInterface.loadDataframe()
        dataframe = self.addDonchianChannel(periods)
        dataframe = dataframe.rename(columns=str.lower)
        if orderType == 'limit':
            entryRules = self.limitCheck(
                dataframe, entryRules, enterLevel, direction)

        dataframe['enter_level'] = enterLevel
        dataframe['enter_rules'] = entryRules
        dataframe['exit_rules'] = exitRules
        dataframe['market_position'] = self.marketPositionGenerator(
            entryRules, exitRules)

        if orderType == 'limit':
            if direction == 'long':
                dataframe.enter_level = dataframe.enter_level.apply(
                    lambda x: self.tickCorrectionDown(x, tick))
                real_entry = np.where(
                    dataframe.open < dataframe.enter_level, dataframe.open, dataframe.enter_level)
                dataframe["entry_price"] = np.where((dataframe.market_position.shift(
                    1) == 0) & (dataframe.market_position == 1), real_entry, np.nan)
            if direction == "short":
                dataframe.enter_level = dataframe.enter_level.apply(
                    lambda x: self.tickCorrectionUp(x, tick))
                real_entry = np.where(
                    dataframe.open > dataframe.enter_level, dataframe.open, dataframe.enter_level)
                dataframe["entry_price"] = np.where((dataframe.market_position.shift(1) == 0) & (dataframe.market_position == 1),
                                                    real_entry, np.nan)

            dataframe["number_of_stocks"] = np.where((dataframe.market_position.shift(1) == 0) & (dataframe.market_position == 1),
                                                     money / real_entry, np.nan)

        dataframe["entry_price"] = dataframe["entry_price"].fillna(
            method='ffill')
        dataframe["events_in"] = np.where((dataframe.market_position == 1) & (
            dataframe.market_position.shift(1) == 0), "entry", "")

        dataframe["number_of_stocks"] = dataframe["number_of_stocks"].apply(
            lambda x: round(x, 0)).fillna(method='ffill')

        if direction == "long":
            dataframe["open_operations"] = (
                dataframe.close - dataframe.entry_price) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where((dataframe.market_position == 1) & (dataframe.market_position.shift(-1) == 0),
                                                    (dataframe.open.shift(-1) -
                                                     dataframe.entry_price)
                                                    * dataframe.number_of_stocks - 2 * fees,
                                                    dataframe.open_operations)

        if direction == "short":
            dataframe["open_operations"] = (
                dataframe.entry_price - dataframe.close) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where((dataframe.market_position == 1) & (dataframe.market_position.shift(-1) == 0),
                                                    (dataframe.entry_price -
                                                     dataframe.open.shift(-1))
                                                    * dataframe.number_of_stocks - 2 * fees,
                                                    dataframe.open_operations)

        dataframe["open_operations"] = np.where(
            dataframe.market_position == 1, dataframe.open_operations, 0)
        dataframe["events_out"] = np.where(
            (dataframe.market_position == 1) & (dataframe.exit_rules == True), "exit", "")
        dataframe["operations"] = np.where((dataframe.exit_rules == True) & (dataframe.market_position == 1),
                                           dataframe.open_operations, np.nan)
        dataframe["closed_equity"] = dataframe.operations.fillna(0).cumsum()
        dataframe["open_equity"] = dataframe.closed_equity + \
            dataframe.open_operations - dataframe.operations.fillna(0)

        numberInitialStocks = money / dataframe.close[0]

        dataframe["B&H"] = numberInitialStocks * \
            (dataframe.close - dataframe.close[0])

        return dataframe

    def crossunder(array1, array2):
        """
        when array1 crosses top-to-bottom array2, the function returns true
        """
        return (array1 < array2) & (array1.shift(1) > array2.shift(2))

    def crossover(array1, array2):
        """
        when array1 crosses bottom-to-top array2, the function returns true
        """
        return (array1 > array2) & (array1.shift(1) < array2.shift(2))
