import datetime
import types
import calendar

weekday_names = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

def datetime_to_str(date):
    return str(date.date()).replace('-', '')

def today_datetime_string():
    return datetime_to_str(datetime.datetime.today())

def str_or_int_to_datetime(date, hour = None):
    date = str(date)
    year = date[0:4]
    month = date[4:6]
    day = date[6:8]
    time = datetime.datetime(year = int(year), month = int(month), day = int(day))
    if hour:
        time = time.replace(hour=int(hour))
    return time

def datetime_to_elastic(datetime):
    return str(datetime).replace(' ', 'T')

def yield_by_date_range(date_start, date_end):
    current_date = str_or_int_to_datetime(date_start) - datetime.timedelta(days = 1)
    date_date_end = str_or_int_to_datetime(date_end) - datetime.timedelta(days = 1)
    while not current_date > date_date_end:
        current_date = current_date + datetime.timedelta(days = 1)
        yield datetime_to_str(current_date)

def get_last_year_weekday(date):
    if isinstance(date, datetime.datetime):
        date = str(date.date()).replace('-', '')
    date = str(date)
    year = date[0:4]
    month = date[4:6]
    day = date[6:8]
    weekday_date = datetime.datetime(year = int(year), month = int(month), day = int(day))
    weekday = weekday_date.weekday()
    last_year_date = weekday_date - datetime.timedelta(days =364)
    last_year_date_weekday = last_year_date.weekday()
    #return {'present': date, "present_weekday": weekday_names[weekday], "past":int(str(last_year_date.date()).replace('-', '')), "past_weekday": weekday_names[last_year_date_weekday]}
    return last_year_date

def get_last_year_weekday(date):
    weekday_date = str_or_int_to_datetime(date)
    last_year_date = weekday_date - datetime.timedelta(days = 364)
    return last_year_date

def get_past_day(date, days):
    date_dt = str_or_int_to_datetime(date)
    return datetime_to_str(date_dt - datetime.timedelta(days = days))

def get_date_monthrange(date = None, last_year = None):
    if not date:
        date = datetime_to_str(datetime.datetime.today())
    year  = int(date[0:4])
    month = date[4:6]
    if last_year:
        year = year-1
    ending_day = calendar.monthrange(year, int(month))[1]
    return (f"{str(year)}{month}01",  f"{str(year)}{month}{str(ending_day)}")

def str_datetime_to_n8(str_datetime):
    return int(str_datetime.split('T')[0].replace('-', ''))

def today_date_n8():
    return datetime_to_str(datetime.datetime.today())

def process_idc_time(idc_time):
    idc_time = str(idc_time)
    return datetime.datetime.strptime(idc_time,'%H%M%S').time()

def process_idc_date(idc_date):
    idc_date = str(idc_date)
    return datetime.datetime.strptime(idc_date,'%Y%m%d').date()

def process_idc_date_and_time(idc_date, idc_time):
    return datetime.datetime.strptime(str(idc_date)+str(idc_time),'%Y%m%d%H%M%S')

def get_fectrx_week_number(fectrx):
    fectrx = str(fectrx)
    week_number = str(str_or_int_to_datetime(fectrx).isocalendar()[1])
    if len(week_number) == 2:
        week_number = '0'+week_number
    elif len(week_number) == 1:
        week_number = '00'+week_number
    return fectrx[:6]+week_number
