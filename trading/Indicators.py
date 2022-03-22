import pandas as pd
import numpy as np


def apply_function_to_series(series, periods, function):
    return series.rolling(periods).apply(function)


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


def add_donchian_channel_up(high_series, periods):
    return apply_function_to_series(high_series, periods, np.max)


def add_donchian_channel_down(low_series, periods):
    return apply_function_to_series(low_series, periods, np.min)


def add_standard_deviation(close_series, periods):
    return apply_function_to_series(close_series, periods, np.std)


def add_mean(close_series, periods):
    return apply_function_to_series(close_series, periods, np.mean)
