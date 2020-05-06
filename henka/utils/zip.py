import os
from zipfile import ZipFile
from .dir import yield_files_from_dir

def extract_all_from_current_dir():
    for file_name in yield_files_from_dir('.zip'):
            with ZipFile(file_name, 'r') as zip_file:
                zip_file.extractall()