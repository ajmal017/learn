import asyncio

import pandas as pd
import tqdm
from ib_insync import IB, Stock, util


# A single async function
async def ohlcAsync(ib, c):
    
    # handle errors
    try:  
        ohlc = await ib.reqHistoricalDataAsync(
            contract=c,
            endDateTime="",
            durationStr="365 D",
            barSizeSetting="1 day",
            whatToShow="Trades",
            useRTH=True
        )
    except AttributeError:
        return None

    # reverse sort to have latest date on top
    if ohlc:  # ohlc has captured something!
        df_ohlc = util.df(ohlc).sort_index(ascending=False).reset_index(drop=True)
        df_ohlc.insert(0, 'symbol', c.symbol)
        return df_ohlc
   

# To catch errors
def onError(reqId, errorCode, errorString, contract):
    if errorCode not in [354, 10090]:
        print(f"ERROR on reqId {reqId} for '{contract.symbol}': {errorString}")
        return None
    else:
        pass
    
""" # Make the contracts
with IB().connect(host='127.0.0.1', port=1300, clientId=0) as ib:
    contracts = ib.qualifyContracts(*[Stock(s, "SMART", "USD") for s in ['AAPL', 'INTC']])

    # The bad egg!!
    contracts.extend([Stock(conId=11758, symbol='RTN', exchange='SMART', 
                    primaryExchange='NYSE', currency='USD', 
                    localSymbol='RTN', tradingClass='RTN')]) """

""" #### This list comprehension code works well ####
with IB().connect(host='127.0.0.1', port=1300, clientId=0) as ib: 
     
    ib.errorEvent += onError
    oh_list = [ib.run(ohlcAsync(ib, c)) for c in contracts]
    ib.errorEvent -= onError
    
    df_ohlc = pd.concat(oh_list).reset_index()
    print(df_ohlc) """

""" #### This for loop code works well too ####
# ohlcAsync with synchronous IB with an error
with IB().connect(host='127.0.0.1', port=1300, clientId=0) as ib:

    ib.errorEvent += onError
    
    oh_list = []
    
    # contracts = [Stock(conId=11758, symbol='RTN', exchange='SMART', primaryExchange='NYSE', currency='USD', localSymbol='RTN', tradingClass='RTN'),
    #              Stock(conId=274105, symbol='SBUX', exchange='SMART', primaryExchange='NASDAQ', currency='USD', localSymbol='SBUX', tradingClass='NMS')]
    
    for c in contracts:
        try:
            oh_list.append(ib.run(ohlcAsync(ib, c)))
        except AttributeError:
            pass 
    df_ohlc = pd.concat(oh_list).reset_index()
    print(df_ohlc) """

async def coro():
    
    with await IB().connectAsync(host='127.0.0.1', port=1300, clientId=0) as ib:

        contracts = await ib.qualifyContractsAsync(*[Stock(s, "SMART", "USD") 
                        for s in ['AAPL', 'INTC', 'BLK', 'ABBV', 'LOWE']])

        # The bad egg!!
        contracts.extend([Stock(conId=11758, symbol='RTN', exchange='SMART', 
                        primaryExchange='NYSE', currency='USD', 
                        localSymbol='RTN', tradingClass='RTN')])
        
        tasks = [ohlcAsync(ib, c) for c in contracts]
        
        return await asyncio.gather(*tasks)
    
oh_list = asyncio.run(coro())
df_ohlc = pd.concat(oh_list).reset_index()
print(df_ohlc)
