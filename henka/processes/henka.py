import pandas as pd
from henka.utils.text import get_replace_function
from henka.utils.bool import get_has_content_function

def process_dataframe(dataframe_config, dataframes):
    if dataframe_config.source_name in ['agg', 'agg_concatenated', 'df', 'join']:
        dataframe_config.set_df_from_dataframe(dataframes)
    df = dataframe_config.dataframe
    config = dataframe_config.process
    save = dataframe_config.save if hasattr(dataframe_config, 'save') else None
    if bool(config.fields):
        df = df[config.fields]

    if hasattr(config, 'remove_duplicates'):
        df = df.drop_duplicates(config.remove_duplicates, keep='first')

    if hasattr(config, 'duplicate_columns'):
        for k, v in config.duplicate_columns.items():
            df[v] = df[k]

    if hasattr(config, 'mock'):
        for column, value in config.mock.items():
            serie =  pd.Series([value for _ in range(0, df.shape[0])])
            serie_df = pd.DataFrame(serie)
            serie_df = serie_df.rename(columns={0: column})
            df = df.join(serie_df)

    if hasattr(config, 'replace_content'):
        for field, dictionary in config.replace_content.items():
            replace_function = get_replace_function(dictionary)
            df[field] = df[field].map(replace_function)

    if hasattr(config, 'keep_row'):
        for field, contents in config.keep_row.items():
            for content in contents:
                has_content = get_has_content_function(content)
                df = df[df[field].apply(has_content)]

    if hasattr(config, 'remove_row'):
        for field, contents in config.remove_row.items():
            for content in contents:
                has_content = get_has_content_function(content, opposite=True)
                df = df[df[field].apply(has_content)]

    if hasattr(config, 'process_content'):
        for function_name, fields in config.process_content.items():
            for field in fields:
                df[field]= df[field].map(config.function_config[function_name])

    if hasattr(config, 'process_content_custom'):
        for function_dict in config.process_content_custom:
            for field in function_dict['fields']:
                df[field]= df[field].map(function_dict['function'])

    if hasattr(config, 'concatenate_columns'):
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

    if hasattr(config, 'delete_columns'):
        for to_delete_column in config.delete_columns:
            df = df.drop(columns=[to_delete_column])

    if hasattr(config, 'rename_columns'):
        if bool(config.rename_columns):
            df = df.rename(columns=config.rename_columns)

    if save:
        save.save_dataframe(df)
    return (dataframe_config.name, df)



def henka(config):
    dataframes = {}
    for dataframe_config in config.dataframes:
        df = process_dataframe(dataframe_config, dataframes)
        dataframes[df[0]] = df[1]
        print(dataframes)
    return dataframes
