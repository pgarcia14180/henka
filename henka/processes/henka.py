import pandas as pd
import numpy as np
from henka.utils.text import get_replace_function
from henka.utils.bool import get_has_content_function
from henka.utils.closure_functions import set_columns_from_arguments

def remove_duplicates(df, config):
    return df.drop_duplicates(config.remove_duplicates, keep='first')

def drop_empty(df, config):
    return df.dropna(subset = config.drop_empty)

def duplicate_columns(df, config):
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
    for column, value in config.mock.items():
        df[column] = value
        """ serie =  pd.Series([value for _ in range(0, df.shape[0])])
        serie_df = pd.DataFrame(serie)
        serie_df = serie_df.rename(columns={0: column})
        df = df.join(serie_df) """ 
    return df

def columns(df, config):
    if bool(config.columns):
        df = df[config.columns]
    return df

def replace_content(df, config):
    for column, dictionary in config.replace_content.items():
        replace_function = get_replace_function(dictionary)
        df[column] = df[column].map(replace_function)
    return df

def keep_row(df, config):
    for column, contents in config.keep_row.items():
        for content in contents:
            has_content = get_has_content_function(content)
            df = df[df[column].apply(has_content)]
    return df

def remove_row(df, config):
    for column, contents in config.remove_row.items():
        for content in contents:
            has_content = get_has_content_function(content, opposite=True)
            df = df[df[column].apply(has_content)]
    return df

def process_content(df, config):
    for function_dict in config.process_content:
        for column in function_dict['columns']:
            df[column]= df[column].map(function_dict['function'])
    return df

def formulate(df, config):
    for function_dict in config.formulate:
        function = set_columns_from_arguments(df, function_dict['function']) 
        df[function_dict['result']] = np.vectorize(function)()
    return df

def concatenate_columns(df, config):
    for new_column_name, dictionary in config.concatenate_columns.items():
        concatenated_serie = None
        if 'separators' in dictionary:
            for i in range(len(dictionary['columns'])):
                if i == 0:
                    concatenated_serie = df[[dictionary['columns'][0], dictionary['columns'][1]]].astype(str).apply(dictionary['separators'][i].join, axis=1)
                elif i > 1:
                    concatenated_serie = concatenated_serie.str.cat(df[dictionary['columns'][i]].astype(str), sep=dictionary['separators'][i-1])
        elif 'separator' in dictionary:
                concatenated_serie = df[dictionary['columns']].astype(str).apply(dictionary['separator'][1].join, axis=1)
        if 'opening' in dictionary:
            concatenated_serie = dictionary['opening']+concatenated_serie
        if 'closing' in dictionary:
            concatenated_serie = concatenated_serie+dictionary['closing']
            df[new_column_name] = concatenated_serie
    return df

def delete_columns(df, config):
    for to_delete_column in config.delete_columns:
        df = df.drop(columns=[to_delete_column])
    return df

def keep_columns(df, config):
    to_delete_columns = set(df.columns.values).difference(config.keep_columns)
    for to_delete_column in to_delete_columns:
        df = df.drop(columns=[to_delete_column])
    return df

def rename_columns(df, config):
    if bool(config.rename_columns):
        df = df.rename(columns=config.rename_columns)
    return df

henka_functions = {
    'remove_duplicates':remove_duplicates,
    'duplicate_columns':duplicate_columns,
    'keep_columns':keep_columns,
    'remove_row': remove_row,
    'mock':mock,
    'replace_content':replace_content,
    'keep_row':keep_row,
    'process_content':process_content,
    'formulate':formulate,
    'concatenate_columns':concatenate_columns,
    'delete_columns':delete_columns,
    'rename_columns':rename_columns,
    'fill_empty': fill_empty,
    'drop_empty': drop_empty,
}

def process_dataframe(dataframe_config, dataframes):
    print(dataframe_config.name)
    if dataframe_config.source_name in ['agg', 'agg_concatenated', 'df', 'join', 'serie']:
        dataframe_config.set_df_from_dataframe(dataframes)
    df = dataframe_config.dataframe
    config = dataframe_config.process
    save = dataframe_config.save if hasattr(dataframe_config, 'save') else None
    drop = dataframe_config.drop if hasattr(dataframe_config, 'drop') else None

    for function_name in config._source.keys():
        df = henka_functions[function_name](df, config)

    if save:
        save.save_dataframe(df)
    if drop:
        return None
    return (dataframe_config.name, df)



def henka(config, **kwargs):
    dataframes = {}
    if kwargs:
        dataframes.update(**kwargs)
    for dataframe_config in config.dataframes:
        df = process_dataframe(dataframe_config, dataframes)
        if df:
            dataframes[df[0]] = df[1]
    return dataframes
