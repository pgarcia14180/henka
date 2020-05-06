import requests
from urllib.parse import urlencode 
from henka.utils.dictionary import DictClass, trim_dictionary

class HttpClient(DictClass):

    def __init__(self, **kwargs):
        super().__init__(**trim_dictionary(kwargs, "user", "password", "url", "params", "file_name"))
        self.session = self.get_session()
        if self.params:
            self.url = self.url+"?"+urlencode(self.params)

    def get_session(self):
        session = requests.Session()
        session.auth = (self.user, self.password)
        return session

    def get_http_data(self):
        return self.session.get(self.url).content
    
    def http_download(self):
        file_name = self.file_name if self.file_name else self.url.rsplit('/', 1)[1]
        open(file_name, "wb").write(self.get_http_data())
        return file_name

