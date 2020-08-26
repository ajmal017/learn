import asyncio
import pickle
from pickle import HIGHEST_PROTOCOL
import pandas as pd
import time
from collections import defaultdict
from datetime import datetime

from ib_insync import *
from ib01_getsyms import get_syms

import sys

# insert original program path for imports
PROGPATH = 'C:/Users/User/Documents/Business/Projeq_cts/asyncib/'
sys.path.insert(1, PROGPATH)

from support import timestr

# time formatter for logs


""" def timestr(end):
    '''Gets string for time in h:m:s

    Arg: 
        end as timedelta
    Returns: 
        xh: ym: zs as string'''

    sec = end.seconds
    hours = sec // 3600
    minutes = (sec // 60) - (hours * 60)
    secs = sec - (minutes * 60) - (hours * 3600)

    return str(hours) + "h: " + str(minutes) + "m: " + str(secs) + "s"
 """

MARKET = 'SNP'

HOST = '127.0.0.1'
PORT = 3000 if MARKET.upper() == 'NSE' else 1300
CID = 0
MASTERCID = 10

# Direct logs to file with level at WARNING (30)
util.logToFile(path='./data/data.log', level=30)
with open('./data/data.log', 'w'):
    pass

# start the clock
start = datetime.now()
print(f"\nStarted at {time.strftime('%X')}\n")

# get all the symbols
df_syms = get_syms(MARKET)

# Qualify underlying contracts

# ...make the symbols unique
symbols = set(df_syms.symbol)

# ...build the contracts
raw_cts = [i for j in [[Stock(symbol, exchange, currency), Index(symbol, exchange, currency)]
                       for symbol, exchange, currency
                       in zip(df_syms.symbol, df_syms.exchange, df_syms.currency)] for i in j]

raw_cts = raw_cts[18:25]  # !!! DATA LIMITER !!!

print(raw_cts)
print('\n')

ib = IB()

# the market data coroutine


async def mktdataCoro(ib, contract):
    '''Coroutine to get a single market data

    Args:
        (ib) as connection object
        (contract) as a single Contract object
    Returns:
        tick as a Ticker object

    '''

    FILL_DELAY = 5  # Delay for filling the ticks

    tick = ib.reqMktData(contract, '456, 104, 106, 100, 101, 165')
    await asyncio.sleep(FILL_DELAY)

    ib.cancelMktData(contract)

    return tick


async def ohlc(ib):

    DURATION = 2  # historical DURATION

    q_task = [ib.qualifyContractsAsync(r) for r in raw_cts]

    sym_data = data = defaultdict(list)

    while q_task:
        q_done, q_pending = await asyncio.wait(q_task,
                                               return_when=asyncio.ALL_COMPLETED)

        for x in q_done:
            q_ct = x.result()

            if q_ct:  # we have a contract!

                c = q_ct[0]

                tasks1 = [
                    asyncio.create_task(mktdataCoro(ib, c), name='MDATA'),
                    ib.reqHistoricalDataAsync(
                        contract=c,
                        endDateTime="",
                        durationStr=str(DURATION) + ' D',
                        barSizeSetting="1 day",
                        whatToShow="Trades",
                        useRTH=True),
                    ib.reqSecDefOptParamsAsync(underlyingSymbol=c.symbol,
                                               futFopExchange="",
                                               underlyingSecType=c.secType,
                                               underlyingConId=c.conId)
                ]

                while tasks1:
                    done, pending = await(asyncio.wait(tasks1,
                                                       return_when=asyncio.FIRST_COMPLETED))

                    for y in done:

                        result = y.result()

                        if result:  # We have some results !!!

                            """ if isinstance(result, Ticker):
                                sym_data[c.symbol].append({'mdata': result})
                            elif isinstance(result[0], BarData):
                                sym_data[c.symbol].append({'ohlc': result})
                            elif isinstance(result[0], Ticker):
                                sym_data[c.symbol].append({'price': result})
                            else:
                                sym_data[c.symbol].append(
                                    {'chain': result[0] if isinstance(result, list) else result}) """

                            if isinstance(result, Ticker):
                                data['mdata'].append(result)
                            elif isinstance(result[0], BarData):
                                data['ohlc'].append(result)
                            elif isinstance(result[0], Ticker):
                                data['price'].append(result)
                            else:
                                data['chain'].append(result)

                            sym_data = {c.symbol: data}

                    tasks1 = pending

        q_task = q_pending

    return sym_data

with ib.connect(HOST, PORT, CID) as ib:
    und_cts = ib.run(ohlc(ib))

print(f"\nThe program ended at {time.strftime('%X')} and " +
      f"took {timestr(datetime.now()-start)}\n")


with open('./data/base_data.pkl', 'wb') as f:
    pickle.dump(und_cts, f, protocol=pickle.HIGHEST_PROTOCOL)

# print(und_cts)
# print('\n')

# print(ib.isConnected())
