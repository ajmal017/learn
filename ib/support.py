import os
from datetime import datetime

import yaml
from ib_insync import util


def delete_all_files(MARKET: str) -> None:
    '''Deletes all data and log files for the market'''

    with open('var.yml') as f:
        data = yaml.safe_load(f)

    LOGPATH = data[MARKET.upper()]["LOGPATH"]
    FSPATH = data[MARKET.upper()]["FSPATH"]

    # deleting data files
    for filename in os.listdir(FSPATH):
        file_path = os.path.join(FSPATH, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path} due to {e}')

    # deleting log files

    logfilepath = os.listdir(LOGPATH[:-4])

    for filename in logfilepath:

        if filename[:3] == MARKET.lower():
            try:
                os.unlink(os.path.join(LOGPATH[:-4], filename))
            except Exception as e:
                print(
                    f'Failed to delete {filename} due to {e}')

    return None


def get_dte(dt):
    '''Gets days to expiry

    Arg: 
        (dt) as day in string format 'yyyymmdd'
    Returns:
        days to expiry as int'''

    return (util.parseIBDatetime(dt) -
            datetime.now().date()).days + 1  # 1 day added to accommodate for US timezone


if __name__ == "__main__":
    delete_all_files('NSE')
