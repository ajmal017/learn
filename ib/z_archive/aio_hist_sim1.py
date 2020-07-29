# test to see how asyncio works with a timeout for one Stock symbol
# note that it does not work for Index symbol (ref: https://groups.io/g/insync/message/4718)

import asyncio

import pandas as pd
from ib_insync import IB, Index, Stock, util

# build up the contracts
HOST = '127.0.0.1'
PORT = '1300'
CID = 0

with IB().connect(HOST, PORT, CID) as ib:
    timeout=5
    # contract = ib.qualifyContracts(Stock(symbol='SBUX', exchange='SMART', currency='USD'))[0]
    contract = ib.qualifyContracts(Index(symbol='VIX', exchange='CBOE', currency='USD'))[0]
    req = ib.reqHistoricalDataAsync(
                contract=contract,
                endDateTime="",
                durationStr="2 D",
                barSizeSetting="1 day",
                whatToShow="Trades",
                useRTH=True)
    try:
        hist = ib.run(asyncio.wait_for(req, timeout))
    except asyncio.exceptions.TimeoutError:
        hist = []

print(contract)
print(hist)
