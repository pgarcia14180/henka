from henka.utils.dictionary import DictClass

class DataframeProcessConfig(DictClass):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
