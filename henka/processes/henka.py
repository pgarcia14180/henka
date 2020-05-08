from .henka_functions import henka_functions
from henka.utils.dataframes import dataframe_to_dict
from henka.utils.constants import sources_from_dataframes

def process_dataframe(dataframe_config, dataframes):
    if dataframe_config.source_name in sources_from_dataframes:
        dataframe_config.set_df_from_dataframe(dataframes)
    df = dataframe_config.dataframe
    config = dataframe_config.get("process")
    save = dataframe_config.get("save")
    append = dataframe_config.get("drop")
    drop = dataframe_config.get("drop")
    as_dictionary = dataframe_config.get("as_dictionary")
    if config:
        for function_name in config.keys(): df = henka_functions[function_name](df, config, dataframes)
    if save: save.save_dataframe(df)
    if as_dictionary: df = dataframe_to_dict(df)
    if drop: return None
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
