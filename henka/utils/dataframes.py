import math

def dataframe_to_dict(df):  
    dictionary_list = []
    for row in df.keys():
        for i, column in enumerate(df[row]):
            if not isinstance(column, str):
                try:
                    if math.isnan(column):
                        column = None
                except TypeError:
                    pass
            try:
                dictionary_list[i].update({row: column})
            except IndexError:
                dictionary_list.append({row: column})
    return dictionary_list