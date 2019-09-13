def convert_value_to_decimal(value):
    value = str(value)
    return float(value[:-2]+'.'+value[-2:])

def divide_by_10(value):
    return value // 10

def convert_to_int(value):
    try:
        return int(value)
    except ValueError as e:
        print(value, e)
        return 0
