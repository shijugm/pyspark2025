import logging
from logging.handlers import RotatingFileHandler ,TimedRotatingFileHandler

def get_logger(name):
    """
    Logger instance.
    This initialises a python logging module and not the pyspark logger
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        # logs to both file and console 
        handlers=[
            # to rotate by size 
            # RotatingFileHandler("logs/application.log",  maxsize=1, backupCount=3),
            # rotate per hour, keep 1 backup
            TimedRotatingFileHandler("logs/application.log", when="H",  backupCount=1),
            logging.StreamHandler()  
        ],
    )
    return logging.getLogger(name)
