import logging
import logging.config

import yaml


class noConsoleFilter(logging.Filter):
    def filter(self, record):
        print("filtering!")
        return not (record.levelname == 'INFO') & ('no-console' in record.msg)

def setupLogging():
    with open('config.yaml', 'r') as f:
        log_cfg = yaml.safe_load(f.read())
        logging.config.dictConfig(log_cfg)    
