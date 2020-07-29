# sets up logging
import json
import logging
import logging.config

import yaml

LOGPATH = "C://Users//User//Documents//Business//Projects//learn//logging//"

class noConsoleFilter(logging.Filter):
    def filter(self, record):
        '''Filters out messages with INFO level and having 'no-console' in its message'''
        print('filter!')
        return not (record.levelname == 'INFO') & ('no-console' in record.msg)

def setupLogging():
    with open(LOGPATH+'log2file.yml', 'rt') as file:
        config = yaml.safe_load(file.read())
        
        print(json.dumps(config, indent=1))
        # logging.config.dictConfig(config)

if __name__ == "__main__":
    
    setupLogging()
