import logging
from datetime import datetime
formatter = logging.Formatter('%(asctime)s : %(message)s')

def setup_logger(name, log_file, level=logging.INFO):
    """Function setup as many loggers as you want"""

    handler = logging.FileHandler(log_file, mode = 'w')        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

default_logger = setup_logger('logger', 'debug.log', level = logging.ERROR)

def report(*messages, logger = None, output = True):
    if not logger:
        logger = default_logger
    if output:
        print(datetime.now().time(), ':', messages)
    logger.critical(str(messages))
