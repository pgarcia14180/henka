import os
from ftplib import FTP
from henka.utils.ftp import (
    download_files_from_ftp_dir, 
    download_file_from_ftp, 
    upload_file_to_ftp, 
    upload_folder_to_ftp,
    rename_file)
from henka.utils.dictionary import trim_dictionary, DictClass
from ..mixins.file_mixin import FileMixin

class FTPClient(DictClass, FileMixin):

    def get_ftp(self):
        return FTP(**trim_dictionary(self, "user", "host", "passwd"))
    
    def download_ftp_files(self):
        params = trim_dictionary(self, "starts_with", "ends_with", "contains", "date_range", "date", "local_folder")
        download_files_from_ftp_dir(self.get_ftp(), self.get("ftp_folder"), **params)

    def download_ftp_file(self):
        download_file_from_ftp(self.get_ftp(), self.file_name, **trim_dictionary(self, "ftp_folder", "local_folder"))

    def ftp_download(self):
        if hasattr(self, 'file_name'):
            self.download_ftp_file()
        else:
            self.download_ftp_files()

    def upload_file(self):
        upload_file_to_ftp(self.get_ftp(), **self)

    def rename_file(self):
        old_name, new_name = self.old_name, self.new_name
        if "ftp_folder" in self:
            ftp_folder = self.ftp_folder+"/"
            old_name, new_name = ftp_folder+old_name, ftp_folder+new_name
        return rename_file(self.get_ftp(), old_name, new_name)