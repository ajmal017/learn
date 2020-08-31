import os
from datetime import datetime
from math import sqrt

import numpy as np
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
        if MARKET.lower() in filename:
            file_path = os.path.join(FSPATH, filename)
        else:
            file_path = None
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
        except Exception as e:
            # print(f'Failed to delete {file_path} due to {e}')
            pass

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


def calcsd(price, undPrice, dte, iv):
    '''Calculate standard deviation for given price.

    Args:
        (price) the price whose sd needs to be known in float
        (undPrice) the underlying price in float
        (dte) the number of days to expiry in int
        (iv) the implied volatility in float

    Returns:
        Std deviation of the price in float

        '''
    try:
        sdev = abs((price - undPrice) / (sqrt(dte / 365) * iv * undPrice))
    except Exception:
        sdev = np.nan
    return sdev


def calcsd_df(price, df):
    '''Back calculate standard deviation for given price. Needs dataframes.

    Args:
        (price) as series of price whose sd needs to be known in float
        (df) as a dataframe with undPrice, dte and iv columns in float

    Returns:
        Series of std deviation as float

        '''
    sdev = (price - df.undPrice) / \
        ((df.dte / 365).apply(sqrt) * df.iv * df.undPrice)
    return abs(sdev)


if __name__ == "__main__":
    delete_all_files('NSE')
