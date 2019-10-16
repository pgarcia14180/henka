from henka.utils.dictionary import DictToClass

class DataframeProcessConfig(DictToClass):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
