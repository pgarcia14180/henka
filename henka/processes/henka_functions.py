import pandas as pd
import numpy as np
from henka.utils.text import get_replace_function
from henka.utils.bool import get_has_content_function
from henka.utils.closure_functions import set_columns_from_arguments
from functools import reduce

def remove_duplicates(df, config):
    """
    Remove duplicates of columns.
    "remove_duplictes": ["column1", "column2"]
    """
    return df.drop_duplicates(config.remove_duplicates, keep='first')

def drop_empty(df, config):
    """
    Remove rows that have nan values
    "drop_empty": ["column1", "column2"]
    """
    return df.dropna(subset = config.drop_empty)

def duplicate_columns(df, config):
    """
    Creates a duplicated column from a column.
    "duplicate_columns": {
        "original_column": ["new_column"]
    }
    """
    for k, vs in config.duplicate_columns.items():
        for v in vs:
            df[v] = df[k]
    return df

def fill_empty(df, config):
    """
    Parameter must be either a value or a dictionary.
    The format of the dictionary is key being the column name, and the value the value to fill the nan.
    When only a value is sent this will fill the whole dataframe if an empty value is found
    """
    if isinstance(config.fill_empty, dict):
        for k, v in config.fill_empty.items():
            df[k] = df[k].fillna(v) 
    else:
        df = df.fillna(config.fill)
    return df

def mock(df, config):
    """
    Creates a column with only one value.
    "mock": {
        "mocked_column": True
    }
    """
    for column, value in config.mock.items():
        df[column] = value
    return df

def columns(df, config):
    "A list that will select wich columns will be used in the dataframe"
    if bool(config.columns):
        df = df[config.columns]
    return df

def replace_content(df, config):
    """
    Replaces the content in the value of the rows. If the content to replace is a
    string, then it replace just that piece, else the entire content.
    "replace_content": {
        "column1": {
            "target1": "result1",
            "target2": "result2",
        }
    }
    """
    case_insensitive = False
    if 'case_insensitive' in config.replace_content:
        case_insensitive = config.replace_content['case_insensitive']
    for column, dictionary in config.replace_content.items():
        if not isinstance(dictionary, bool):
            replace_function = get_replace_function(dictionary)
            df[column] = df[column].map(replace_function)
    return df

def keep_rows(df, config):
    """
    Deletes all rows except the ones found in the conditions.
    If the value is a string, then will check if the string is contained in the value
    "keep_rows": {
        "column": ["value",]
    }
    """
    dfs = []
    case_insensitive = False
    if 'case_insensitive' in config.keep_rows and isinstance(config.keep_rows["case_insensitive"], bool):
        case_insensitive = config.keep_rows['case_insensitive']
    for column, contents in config.keep_rows.items():
        if not isinstance(contents, bool):
            for content in contents:
                has_content = get_has_content_function(content)
                dfs.append(df[df[column].apply(has_content)])
    df = reduce(lambda a, b: pd.concat([a, b]), dfs)
    return df

def remove_rows(df, config):
    """
    Deletes the rows except found in the conditions.
    If the value is a string, then will see if the string is contained in the value
    "remove_rows": {
        "column": ["value",]
    }
    """
    case_insensitive = False
    if 'case_insensitive' in config.remove_rows:
        case_insensitive = config.remove_rows['case_insensitive']
    for column, contents in config.remove_rows.items():
        if not isinstance(contents, bool):
            for content in contents:
                has_content = get_has_content_function(content, opposite=True, case_insensitive=case_insensitive)
                df = df[df[column].apply(has_content)]
    return df

def process_content(df, config):
    """
    Applies a function to each value of the column
    "process_content": [
        {
            "function": lambda x: str(x),
            "columns": ["column1", "column2"]
        }
    ]
    """
    for function_dict in config.process_content:
        for column in function_dict['columns']:
            df[column] = df[column].map(function_dict['function'])
    return df

def formulate(df, config):
    """
    Applies a vectorized function to series, giving a better performance.
    But all the values are series.
    The name of the parameters has to be the same as the columns, therefore
    the name of the columns cannot contain spaces
    "formulate": [
        {
            "function": lambda column1, column2: column1.astype(str)+column2.astype(str),
            "result": "column3"
        }
    ]
    """
    for function_dict in config.formulate:
        function = set_columns_from_arguments(df, function_dict['function']) 
        df[function_dict['result']] = np.vectorize(function)()
    return df

def concatenate_columns(df, config):
    """
    Takes multiple columns, converts them in string, and concatenate them.
    It take extra arguments, opening and closing to wrap the columns.
    "concatenate_columns": {
        "new_column": {
            "columns": ["column1", "column2", "column3"],
            "separators": ["!", "."],
            "opening": "-",
            "closing": ".."
        }
    }
    """
    for new_column_name, dictionary in config.concatenate_columns.items():
        concatenated_series = None
        if 'separators' in dictionary:
            for i in range(len(dictionary['columns'])):
                if i == 0:
                    concatenated_series = df[[dictionary['columns'][0], dictionary['columns'][1]]].astype(str).apply(dictionary['separators'][i].join, axis=1)
                elif i > 1:
                    concatenated_series = concatenated_series.str.cat(df[dictionary['columns'][i]].astype(str), sep=dictionary['separators'][i-1])
        elif 'separator' in dictionary:
                concatenated_series = df[dictionary['columns']].astype(str).apply(dictionary['separator'][1].join, axis=1)
        if 'opening' in dictionary:
            concatenated_series = dictionary['opening']+concatenated_series
        if 'closing' in dictionary:
            concatenated_series = concatenated_series+dictionary['closing']
            df[new_column_name] = concatenated_series
    return df

def remove_columns(df, config):
    """
    Delete specified columns
    "remove_columns": ["column1", "column2"]
    """
    for to_remove_column in config.remove_columns:
        df = df.drop(columns=[to_remove_column])
    return df

def keep_columns(df, config):
    """
    Delete all columns except the ones specified in the list
    "keep_columns": ["column1", "column2"]
    """
    to_remove_columns = set(df.columns.values).difference(config.keep_columns)
    for to_remove_column in to_remove_columns:
        df = df.drop(columns=[to_remove_column])
    return df

def rename_columns(df, config):
    """
    Rename columns, key is the original andd value is the new name
    "rename_columns": {
        "original_name": "new_name"
    }
    """
    if bool(config.rename_columns):
        df = df.rename(columns=config.rename_columns)
    return df

henka_functions = {
    'remove_duplicates':remove_duplicates,
    'duplicate_columns':duplicate_columns,
    'keep_columns':keep_columns,
    'remove_rows': remove_rows,
    'mock':mock,
    'replace_content':replace_content,
    'keep_rows':keep_rows,
    'process_content':process_content,
    'formulate':formulate,
    'concatenate_columns':concatenate_columns,
    'remove_columns':remove_columns,
    'rename_columns':rename_columns,
    'fill_empty': fill_empty,
    'drop_empty': drop_empty,
}
