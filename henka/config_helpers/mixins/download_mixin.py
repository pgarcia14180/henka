from ..clients.ftp_client import FTPClient
from ..clients.http_client import HttpClient
from henka.utils.dictionary import DictClass

class DownloadMixin():

    def download(self):
        if "download_from" in self:
            if self.download_from == "ftp":
                FTPClient(**self).ftp_download()
            elif self.download_from == "http":
                HttpClient(**self).http_download()
        