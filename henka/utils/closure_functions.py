import pandas as pd 

def set_columns_from_arguments(df: pd.DataFrame, function):
    series = []
    for column_name in function.__code__.co_varnames:
        series.append(df[column_name])
    def new_function():
        nonlocal series
        return function(*series)
    return new_function
