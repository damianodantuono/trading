import pandas as pd
import numpy as np


def apply_function_to_series(series, periods, function):
    return series.rolling(periods).apply(function)


def crossunder(array1, array2):
    return (array1 < array2) & (array1.shift(1) > array2.shift(2))


def crossover(array1, array2):
    return (array1 > array2) & (array1.shift(1) < array2.shift(2))


def add_donchian_channel_up(high_series, periods):
    return apply_function_to_series(high_series, periods, np.max)


def add_donchian_channel_down(low_series, periods):
    return apply_function_to_series(low_series, periods, np.min)


def add_standard_deviation(close_series, periods):
    return apply_function_to_series(close_series, periods, np.std)


def add_mean(close_series, periods):
    return apply_function_to_series(close_series, periods, np.mean)


def add_stoploss(entry_level: pd.Series, enter_rule: pd.Series, exit_rule: pd.Series, stop_loss: float) -> pd.Series:
    df = pd.DataFrame({'entry_level': entry_level, 'enter_rule': enter_rule, 'exit_rule': exit_rule})
    df = df.sort_index()
    for index, row in df.iterrows():
        if row['enter_rule']:
            pass
