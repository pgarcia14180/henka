import os
import time
import pandas as pd
import numpy as np
from .dataframes import dataframe_to_dict
from .dictionary import DictClass, unpack_dictionary_values
from .dir import yield_files_from_dir
from .text import filter_text
from datetime import datetime
from dateutil import parser

def yield_ftp_dir_files(ftp, folder_name, **kwargs):
    if folder_name: ftp.cwd('/'+folder_name)
    ftp_raw_data = []
    data = []
    confusing_year_mask = []
    unambiguous_year_mask = []
    ftp.dir(ftp_raw_data.append)
    now = datetime.now()
    for lines in ftp_raw_data:
        rows = lines.split(None, 8)
        timestamp = parser.parse(rows[5] + " " + rows[6] + " " + rows[7])
        has_time = True if ":" in rows[7] else False
        data.append([rows[8], timestamp])
        confusing_year = True if has_time and now.month < timestamp.month else False
        confusing_year_mask.append(confusing_year)
        unambiguous_year_mask.append(not confusing_year)
    df = pd.DataFrame(data, columns = ["file_name", "timestamp"])
    ambiguous_year_series = df[confusing_year_mask]["timestamp"] - pd.DateOffset(years=1)
    unambiguous_year_series = df[unambiguous_year_mask]["timestamp"] 
    df["timestamp"] = pd.DataFrame(
        np.hstack((ambiguous_year_series.values, unambiguous_year_series.values)),
        index = np.hstack((ambiguous_year_series.index.values, unambiguous_year_series.index.values))
    )[0]
    date_range, date = unpack_dictionary_values(kwargs, "date_range", "date")
    if date_range:
        gte, lte, gt, lt = unpack_dictionary_values(date_range, 'gte', 'lte', 'gt', 'lt')
        if gte: df = df[df["timestamp"] >= parser.parse(gte)]
        if gt: df = df[df["timestamp"] > parser.parse(gt)]
        if lte: df = df[df["timestamp"] <= parser.parse(lte)]
        if lt: df = df[df["timestamp"] < parser.parse(lt)]
    return (row["file_name"] for row in dataframe_to_dict(df))

def download_file_from_ftp(ftp, file_name, **kwargs):
    save_location = kwargs.get("local_folder")+"/"+file_name if kwargs.get("local_folder") else file_name
    with open(save_location, 'wb') as f:
        ftp.retrbinary('RETR ' + file_name, f.write)

def download_files_from_ftp_dir(ftp, folder_name, file_name = None, starts_with = None, contains = None, ends_with = None, **kwargs):
    for file_name in yield_ftp_dir_files(ftp, folder_name, **kwargs):
        if filter_text(file_name, starts_with= starts_with, contains=contains, ends_with= ends_with): 
            download_file_from_ftp(ftp, file_name, **kwargs)

def upload_file_to_ftp(ftp, **kwargs):
    file_name = kwargs["file_name"]
    ftp_folder = kwargs.get("ftp_folder")+"/" if kwargs.get("ftp_folder") else ""
    local_folder = kwargs.get("local_folder")+"/" if kwargs.get("local_folder") else ""
    local_file = local_folder+file_name
    ftp_file = ftp_folder+file_name
    with open(local_file, 'rb') as file:
        ftp.storbinary('STOR '+ftp_file, file)
        time.sleep(1)

def upload_folder_to_ftp(ftp, **kwargs):
    for file_name in yield_files_from_dir():
        upload_file_to_ftp(ftp, **kwargs)

def rename_file(ftp, old_name, new_name):
    return ftp.rename(old_name, new_name)