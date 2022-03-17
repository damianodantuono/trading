import datetime
from operator import le
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import constants
import plot
import math


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


def load_data_intraday(filename):
    """
    Caricamento di uno storico di dati intraday
    """
    data = pd.read_csv(filename, parse_dates=[['date', 'time']], index_col=0, usecols=[
                       'date', 'time', 'open', 'high', 'low', 'close', 'up', 'down'])
    data['volume'] = data.up + data.down
    data.drop(['up', 'down'], axis=1, inplace=True)
    data['dayofweek'] = data.index.dayofweek
    data['day'] = data.index.day
    data['month'] = data.index.month
    data['year'] = data.index.year
    data['dayofyear'] = data.index.dayofyear
    data['quarter'] = data.index.quarter
    data['hour'] = data.index.hour
    data['minute'] = data.index.minute
    return data


def load_data_daily(filename):
    """
    Caricamento di uno storico di dati
    """
    data = pd.read_csv(filename, parse_dates=['date'], index_col='date', usecols=[
                       'date', 'open', 'high', 'low', 'close', 'volume'])
    data['dayofweek'] = data.index.dayofweek
    data['day'] = data.index.day
    data['month'] = data.index.month
    data['year'] = data.index.year
    data['dayofyear'] = data.index.dayofyear
    data['quarter'] = data.index.quarter
    return data


def add_donchian_channel(dataframe, values):
    """
    Add donchian channel for N windows.
    donchian channel: maximum of last N maxima and minimum of last N minima

    Args:
        dataframe (pandas.DataFrame): dataframe cointaining OHLC data.
    """
    values = set(values)

    tmp_dataframe = dataframe.copy()
    tmp_dataframe = tmp_dataframe.rename(columns=str.lower)
    for value in values:
        dataframe[f'hhv{value}'] = tmp_dataframe.high.rolling(value).max()
        dataframe[f'llv{value}'] = tmp_dataframe.low.rolling(value).min()

    dataframe.dropna(inplace=True)
    return dataframe


def add_moving_avg(dataframe, values):
    """
    Add simple moving average for N windows.

    Args:
        dataframe (pandas.DataFrame): dataframe cointaining OHLC data.
    """
    values = set(values)

    tmp_dataframe = dataframe.copy()
    tmp_dataframe = tmp_dataframe.rename(columns=str.lower)
    for value in values:
        dataframe[f'sma{value}'] = tmp_dataframe.close.rolling(value).mean()

    dataframe.dropna(inplace=True)
    return dataframe


def tick_correction_up(level, tick):
    if level != level:
        level = 0
    multipier = math.ceil(level/tick)
    return multipier * tick


def tick_correction_down(level, tick):
    if level != level:
        level = 0
    multipier = math.floor(level/tick)
    return multipier * tick


def stop_check(dataframe, rules, level, direction):
    service_dataframe = pd.DataFrame(index=dataframe.index)
    service_dataframe['rules'] = rules
    service_dataframe['level'] = level
    service_dataframe['low'] = dataframe.low
    service_dataframe['high'] = dataframe.high

    if direction == 'long':
        service_dataframe['new_rules'] = np.where((service_dataframe.rules == True) & (
            service_dataframe.high.shift(-1) >= service_dataframe.level.shift(-1)), True, False)

    if direction == 'short':
        service_dataframe['new_rules'] = np.where((service_dataframe.rules == True) & (
            service_dataframe.low.shift(-1) <= service_dataframe.level.shift(-1)), True, False)

    return service_dataframe.new_rules


def limit_check(dataframe, rules, level, direction):
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


def marketposition_generator(enter_rule, exit_rule):
    """
    Generate market position, given both rules

    Args:
        enter_rule (pandas.Series): boolean series of entry rules
        exit_rule (pandas.Series): boolean series of exit rules
    """

    frame = {'enter_rule': enter_rule, 'exit_rule': exit_rule}
    tmp_dataframe = pd.DataFrame(frame)
    status = 0
    mp = []
    for i, j in zip(enter_rule, exit_rule):
        if status == 0:
            if i and not j:
                status = 1
        else:
            if j:
                status = 0
        mp.append(status)
    tmp_dataframe['mp_new'] = mp
    tmp_dataframe.mp_new = tmp_dataframe.mp_new.shift(1)
    tmp_dataframe.iloc[0, 2] = 0
    # tmp_dataframe.to_csv('marketposition_generator.csv')
    return tmp_dataframe.mp_new


def apply_trading_system(imported_dataframe, bigpointvalue, tick, direction, order_type, enter_level, enter_rules, exit_rules):
    dataframe = imported_dataframe.copy()
    if constants.ORDER_TYPE == "stop":
        enter_rules = stop_check(
            dataframe, enter_rules, enter_level, direction)
    if constants.ORDER_TYPE == "limit":
        enter_rules = limit_check(
            dataframe, enter_rules, enter_level, direction)

    dataframe['enter_level'] = enter_level
    dataframe['enter_rules'] = enter_rules
    dataframe['exit_rules'] = exit_rules

    dataframe["mp"] = marketposition_generator(
        dataframe.enter_rules, dataframe.exit_rules)

    if constants.ORDER_TYPE == "market":
        dataframe["entry_price"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                            dataframe.open, np.nan)
        if constants.INSTRUMENT == 1:
            dataframe["number_of_stocks"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                     constants.OPERATION_MONEY / dataframe.open, np.nan)
    if constants.ORDER_TYPE == "stop":
        if direction == "long":
            dataframe.enter_level = dataframe.enter_level.apply(
                lambda x: tick_correction_up(x, tick))
            real_entry = np.where(
                dataframe.open > dataframe.enter_level, dataframe.open, dataframe.enter_level)
            dataframe["entry_price"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                real_entry, np.nan)
        if direction == "short":
            dataframe.enter_level = dataframe.enter_level.apply(
                lambda x: tick_correction_down(x, tick))
            real_entry = np.where(
                dataframe.open < dataframe.enter_level, dataframe.open, dataframe.enter_level)
            dataframe["entry_price"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                real_entry, np.nan)
        if constants.INSTRUMENT == 1:
            dataframe["number_of_stocks"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                     constants.OPERATION_MONEY / real_entry, np.nan)
    if constants.ORDER_TYPE == "limit":
        if direction == "long":
            dataframe.enter_level = dataframe.enter_level.apply(
                lambda x: tick_correction_down(x, tick))
            real_entry = np.where(
                dataframe.open < dataframe.enter_level, dataframe.open, dataframe.enter_level)
            dataframe["entry_price"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                real_entry, np.nan)
        if direction == "short":
            dataframe.enter_level = dataframe.enter_level.apply(
                lambda x: tick_correction_up(x, tick))
            real_entry = np.where(
                dataframe.open > dataframe.enter_level, dataframe.open, dataframe.enter_level)
            dataframe["entry_price"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                real_entry, np.nan)
        if constants.INSTRUMENT == 1:
            dataframe["number_of_stocks"] = np.where((dataframe.mp.shift(1) == 0) & (dataframe.mp == 1),
                                                     constants.OPERATION_MONEY / real_entry, np.nan)

    dataframe["entry_price"] = dataframe["entry_price"].fillna(method='ffill')
    dataframe["events_in"] = np.where((dataframe.mp == 1) & (
        dataframe.mp.shift(1) == 0), "entry", "")
    if constants.INSTRUMENT == 1:
        dataframe["number_of_stocks"] = dataframe["number_of_stocks"].apply(lambda x: round(x, 0))\
            .fillna(method='ffill')

    if direction == "long":
        if constants.INSTRUMENT == 1:
            dataframe["open_operations"] = (
                dataframe.close - dataframe.entry_price) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where((dataframe.mp == 1) & (dataframe.mp.shift(-1) == 0),
                                                    (dataframe.open.shift(-1) -
                                                     dataframe.entry_price)
                                                    * dataframe.number_of_stocks - 2 * constants.COSTS,
                                                    dataframe.open_operations)
        if constants.INSTRUMENT == 2:
            dataframe["open_operations"] = (
                dataframe.close - dataframe.entry_price) * bigpointvalue
            dataframe["open_operations"] = np.where((dataframe.mp == 1) & (dataframe.mp.shift(-1) == 0),
                                                    (dataframe.open.shift(-1) -
                                                     dataframe.entry_price)
                                                    * bigpointvalue - 2 * constants.COSTS,
                                                    dataframe.open_operations)
    if direction == "short":
        if constants.INSTRUMENT == 1:
            dataframe["open_operations"] = (
                dataframe.entry_price - dataframe.close) * dataframe.number_of_stocks
            dataframe["open_operations"] = np.where((dataframe.mp == 1) & (dataframe.mp.shift(-1) == 0),
                                                    (dataframe.entry_price -
                                                     dataframe.open.shift(-1))
                                                    * dataframe.number_of_stocks - 2 * constants.COSTS,
                                                    dataframe.open_operations)
        if constants.INSTRUMENT == 2:
            dataframe["open_operations"] = (
                dataframe.entry_price - dataframe.close) * bigpointvalue
            dataframe["open_operations"] = np.where((dataframe.mp == 1) & (dataframe.mp.shift(-1) == 0),
                                                    (dataframe.entry_price -
                                                     dataframe.open.shift(-1))
                                                    * bigpointvalue - 2 * constants.COSTS,
                                                    dataframe.open_operations)
    dataframe["open_operations"] = np.where(
        dataframe.mp == 1, dataframe.open_operations, 0)
    dataframe["events_out"] = np.where(
        (dataframe.mp == 1) & (dataframe.exit_rules == True), "exit", "")
    dataframe["operations"] = np.where((dataframe.exit_rules == True) & (dataframe.mp == 1),
                                       dataframe.open_operations, np.nan)
    dataframe["closed_equity"] = dataframe.operations.fillna(0).cumsum()
    dataframe["open_equity"] = dataframe.closed_equity + \
        dataframe.open_operations - dataframe.operations.fillna(0)
    dataframe.to_csv("trading_system_export.csv")
    return dataframe


def drawdown(equity):
    """
    Compute draw down of equity

    Args:
        equity (pandas.Series): equity
    """
    maxvalue = equity.expanding(0).max()
    drawdown = pd.Series(equity - maxvalue, index=equity.index)
    return drawdown


def max_drawdown(equity):
    return round(drawdown(equity).min(), 2)


def avg_drawdown(equity):
    dd = drawdown(equity)
    return round(dd[dd < 0].mean(), 2)


def avg_loss(operations):
    operations = operations.dropna()
    return round(operations[operations < 0].mean(), 2)


def max_loss(operations):
    operations = operations.dropna()
    return operations.idxmin(), round(operations.min(), 2)


def avg_gain(operations):
    operations = operations.dropna()
    return round(operations[operations > 0].mean(), 2)


def max_gain(operations):
    operations = operations.dropna()
    return operations.idxmax(), round(operations.max(), 2)


def gross_profit(operations):
    operations = operations.dropna()
    return round(operations[operations > 0].sum(), 2)


def gross_loss(operations):
    operations = operations.dropna()
    return round(operations[operations <= 0].sum(), 2)


def profit_factor(operations):
    profit = gross_profit(operations)
    loss = gross_loss(operations)
    if loss != 0:
        return round(abs(profit/loss), 2)
    return np.inf


def percent_win(operations):
    operations = operations.dropna()
    return round((operations[operations > 0].count() / operations.count()) * 100, 2)


def reward_risk_ratio(operations):
    operations = operations.dropna()
    if operations[operations < 0].mean() != 0:
        return round(abs(operations[operations > 0].mean() / operations[operations <= 0].mean()), 2)
    return np.inf


def profit(equity):
    return round(equity[-1], 2)


def number_of_operations(operations):
    return operations.dropna().count()


def operation_stats(operations):
    operations = operations.dropna()
    return {'mean': round(operations.mean(), 2), 'std': round(operations.std(), 2)}


def delay_between_peaks(equity):
    work_df = pd.DataFrame(equity, index=equity.index)
    work_df['drawdown'] = drawdown(equity)
    work_df['delay_element'] = work_df['drawdown'].apply(
        lambda x: 1 if x < 0 else 0)
    work_df['resets'] = np.where(work_df['drawdown'] == 0, 1, 0)
    work_df['cumsum'] = work_df['resets'].cumsum()
    return pd.Series(work_df['delay_element'].groupby(work_df['cumsum']).cumsum())


def max_delay_between_peaks(equity):
    return delay_between_peaks(equity).max()


def avg_delay_between_peaks(equity):
    work_df = pd.DataFrame(equity, index=equity.index)
    work_df['drawdown'] = drawdown(equity)
    work_df['delay_element'] = work_df['drawdown'].apply(
        lambda x: 1 if x < 0 else np.nan)
    work_df['resets'] = np.where(work_df['drawdown'] == 0, 1, 0)
    work_df['cumsum'] = work_df['resets'].cumsum()
    work_df.dropna(inplace=True)
    a = work_df['delay_element'].groupby(work_df['cumsum']).cumsum()
    return round(a.mean(), 2)


def performance_report(trading_system):
    output = f"""
    Performance Report

    Profit:                  , {profit(trading_system.open_equity)}
    Operations:              , {number_of_operations(trading_system.operations)}
    Average Trade:           , {trading_system.operations.dropna().mean().__round__(2)}

    Profit Factor:           , {profit_factor(trading_system.operations)}
    Gross Profit:            , {gross_profit(trading_system.operations)}
    Gross Loss:              , {gross_loss(trading_system.operations)}

    Percent Winning Trades:  , {percent_win(trading_system.operations)} %
    Percent Losing Trades:   , {100 - percent_win(trading_system.operations)} %
    Reward Risk Ratio:       , {reward_risk_ratio(trading_system.operations)}

    Max Gain:                , {max_gain(trading_system.operations)[1]},  in date , {max_gain(trading_system.operations)[0]}
    Average Gain:            , {avg_gain(trading_system.operations)}
    Max Loss:                , {max_loss(trading_system.operations)[1]},  in date , {max_loss(trading_system.operations)[0]}
    Average Loss:            , {avg_loss(trading_system.operations)}

    Avg Open Draw Down:      , {avg_drawdown(trading_system.open_equity)}
    Max Open Draw Down:      , {max_drawdown(trading_system.open_equity)}

    Avg Closed Draw Down:    , {avg_drawdown(trading_system.closed_equity)}
    Max Closed Draw Down:    , {max_drawdown(trading_system.closed_equity)}

    Avg Delay Between Peaks: , {avg_delay_between_peaks(trading_system.open_equity)}
    Max Delay Between Peaks: , {max_delay_between_peaks(trading_system.open_equity)}
    """
    with open('report\\report.txt', 'w') as f:
        f.write(output)

    plot.plot_equity(trading_system.open_equity, "green", 'report\\equity.png')
    plot.plot_drawdown(trading_system.open_equity,
                       "red", 'report\\drawdown.png')
    plot.plot_annual_histogram(
        trading_system.operations, 'report\\annualHistogram.png')
    plot.plot_monthly_histogram(
        trading_system.operations, 'report\\annualHistogram.png')
    plot.plot_equity_heatmap(trading_system.operations,
                             False, 'report\\heatmap.png')
    return
