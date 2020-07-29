import sys

import pandas as pd
from ib_insync import *

# insert original program path for imports
PROGPATH = 'C:/Users/User/Documents/Business/Projects/asyncib/'
DATAPATH = './data/'
sys.path.insert(1, PROGPATH)

from support import get_nse, get_snp  # have this below sys.path.insert!!!


def get_syms(MARKET):

    # Get symbols
    df_syms = get_nse() if MARKET.upper() == 'NSE' else get_snp()

    # Pickle
    df_syms.to_pickle(DATAPATH + 'df_' + MARKET.lower() + 'syms.pkl')

    return df_syms


if __name__ == "__main__":

    get_syms('NSE')
