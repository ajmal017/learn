import asyncio
import pickle
from datetime import datetime

from ib_insync import IB, Index, Stock, util
from tqdm import tqdm

from ib01_getsyms import get_syms
from support import timestr

MARKET = 'SNP'

HOST = '127.0.0.1'
PORT = 4002 if MARKET.upper() == 'NSE' else 4002  # Paper trades!!
CID = 0

DATAPATH = './data/'
LONG_BAR_FORMAT = '{desc:<25}{percentage:3.0f}%|{bar:30}{r_bar}'

QBLK = 50  # block chunks for qualification
start = datetime.now()

# Suppress errors in console
logfile = './data/test.log'
with open(logfile, 'w'):
    pass
util.logToFile(logfile, level=30)

# Get the symbols
df_syms = get_syms(MARKET)

# Build the contracts
raw_cts = [i for j in [[Stock(symbol, exchange, currency), Index(symbol, exchange, currency)]
                       for symbol, exchange, currency
                       in zip(df_syms.symbol, df_syms.exchange, df_syms.currency)] for i in j]

c_blks = [raw_cts[i: i + QBLK] for i in range(0, len(raw_cts), QBLK)]

tq_blks = tqdm(c_blks, position=0, leave=True,
               desc=f'Qualifying {len([i for c in c_blks for i in c])} ',
               bar_format=LONG_BAR_FORMAT)

qunds = dict()  # to store qualified underlying contracts

# Qualify the underlyings
with IB().connect(HOST, PORT, CID) as ib:
    for b in tq_blks:
        cts = ib.qualifyContracts(*b)
        qunds.update({r.symbol: r for r in cts if r})
        tq_blks.refresh()

with open(DATAPATH + 'und_cts.pkl', 'wb') as f:
    pickle.dump(qunds, f)

# Coros for OHLC, price, marketdata and chains


async def ohlc_coro():


print(f"Took {timestr(datetime.now() - start)}")
