# test to see how asyncio works with a timeout for many symbols.
# Index symbol (ref: https://groups.io/g/insync/message/4718)
# Index symbols have also been put into the mix!

import asyncio
import time

import pandas as pd
from ib_insync import IB, Index, Stock, util

HOST = '127.0.0.1'
PORT = '1300'
CID = 0

""" contracts = ib.qualifyContracts(*[
Stock(symbol='SBUX', exchange='SMART', currency='USD'),
Stock(symbol='AAPL', exchange='SMART', currency='USD'),
Stock(symbol='INTC', exchange='SMART', currency='USD'),
Index(symbol='VIX', exchange='CBOE', currency='USD'),
Index(symbol='MXEA', exchange='CBOE', currency='USD')
])
ib.sleep(0.1) """
       
start_time = time.time()

# Get the contracts
contracts = pd.read_pickle("C:/Users/User/Documents/Business/Projects/asyncib/data/snp/df_syms.pkl").contract.dropna()
duration = 365

async def gethists(ib, contracts, duration, timeout):

    dur_str = str(duration)+ ' D'

    block = 20  # split into blocks to avoid max message per second error        
    c_blks = [contracts[i: i+block] for i in range(0, len(contracts), block)]  
  
    # split contracts to {symbol: future_pending} dictionary
    aiolists = [{c.symbol: ib.reqHistoricalDataAsync(
        contract=c,
        endDateTime="",
        durationStr=dur_str,
        barSizeSetting="1 day",
        whatToShow="Trades",
        useRTH=True) for c in cs} for cs in c_blks]
    
    histlist = []
    
    for aios in aiolists:
        for k, v in aios.items():
            try:
                hist = await asyncio.wait_for(v, timeout)
                df = util.df(hist)
                df.insert(0, 'symbol', k)
            except asyncio.TimeoutError:
                print(f"Timeout exception for {k}")
                df = pd.DataFrame([{'symbol': k}])
            except Exception as e:
                print(f"Some other error {e}")  
                df = pd.DataFrame([{'symbol': k}])
                
            histlist.append(df)
            
    return histlist

with IB().connect(HOST, PORT, CID) as ib:
    dfs = ib.run(gethists(ib, contracts, duration=365, timeout=95)) # First run
    
    missing_cs = [c for c in contracts if c not in pd.concat(dfs).symbol.unique()]
    dfs2 = ib.run(gethists(ib, missing_cs, duration=365, timeout=3))

    
df_ohlc = pd.concat(dfs+dfs2).set_index('symbol').drop_duplicates()

print(df_ohlc)
print(f"\n----{time.time()-start_time:.2f} seconds")

# df_ohlc.to_pickle('./temp.pkl')
