import os
from henka.utils.text import filter_text
from henka.utils.dir import yield_files_from_dir
from henka.utils.dictionary import trim_dictionary
from henka.utils.constants import to_ignore_extensions

class FileMixin():

    def yield_files_names_from_dir(self):
        params = trim_dictionary(self, "folder_name", "starts_with", "ends_with", "contains")
        return (file_name for file_name in yield_files_from_dir() if filter_text(file_name, **params))
    
    def yield_files(self):
        for file_name in self.yield_files_names_from_dir():
            yield file_name
        if hasattr(self, "file_name"):
            yield self.file_name

    def remove_local_files(self):
        if hasattr(self, "_remove_local_files") and self.remove_local_files:
            for file_name in self.yield_files():
                self.remove_file(file_name)
    
    def remove_file(self, file_name):
        to_ignore_files = self.get("ignore")
        if not file_name in to_ignore_files:
            for extension in to_ignore_extensions:
                if not file_name.endswith(extension) and os.path.exists(file_name):
                    os.remove(file_name)