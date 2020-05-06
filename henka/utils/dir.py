import os

def yield_files_from_dir(dir = None):
    return (file_name for file_name in os.listdir(dir))