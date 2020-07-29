# posted as a question to https://stackoverflow.com/questions/61456543

import logging
import logging.config

import yaml


class noConsoleFilter(logging.Filter):
    def filter(self, record):
        print("filtering!")
        return not (record.levelname == 'INFO') & ('no-console' in record.msg)

with open('config.yaml', 'r') as f:
    log_cfg = yaml.safe_load(f.read())
    logging.config.dictConfig(log_cfg)  

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logger.info("no-console. Should not be in console, but be in test.log!")
logger.info('This is an info message')
logger.error('This is an error message')
