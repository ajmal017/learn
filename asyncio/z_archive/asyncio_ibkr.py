# Experimental program to evaluate asyncio in IBKR
# Program needs iboop's _df_unds.pkl for underlying contracts

import asyncio
import pickle
from itertools import product

import numpy as np
import pandas as pd
import yaml
from ib_insync import IB, Dividends, Index, Stock, util


# time formatter for logs
def timestr(end):
    """Gets string for time in h:m:s
    Arg: end as timedelta
    Returns: xh: ym: zs as string"""

    sec = end.seconds
    hours = sec // 3600
    minutes = (sec // 60) - (hours * 60)
    secs = sec - (minutes * 60) - (hours * 3600)

    return str(hours) + "h: " + str(minutes) + "m: " + str(secs) + "s"

async def ohlcAsync(ib, c):
    """Args:
        (ib) as connection object
        (c) as a single contract
    Returns:
        dictionary of {conId: df_ohlc dataframe}"""
    
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
        df_ohlc = df_ohlc.assign(conId = c.conId)
        return {c.conId: df_ohlc}

async def chainsAsync(ib, contract):
    
    c = await ib.reqSecDefOptParamsAsync(
        underlyingSymbol=contract.symbol,
        futFopExchange="",
        underlyingSecType=contract.secType,
        underlyingConId=contract.conId)
    
    # handle errors
    try:
        cek = list(product([c[0].underlyingConId], c[0].expirations, c[0].strikes))
    except IndexError:
        return None
       
    df_cek = pd.DataFrame(cek, columns=['conId', 'expiry', 'strike']).drop_duplicates().reset_index(drop=True)
    return {contract.conId: df_cek}


async def priceAsync(ib, c):
    """
    Args:
        (ib) as a connection object
        (c) as contract
    Returns:
        dictionary of {conId: price} """
    tickers = await ib.reqTickersAsync(c)
    await asyncio.sleep(2)
    price = {a.contract.conId: a.marketPrice() for a in tickers}
    return price
 
async def mktDataAsync(ib, contract):
    """
    Args:
        (ib) as a connection object
        (c) as contract
    Returns:
        mktdata as a {conId: {parameter: value}} dictionary """    
    
    md = ib.reqMktData(contract, '100, 101, 104, 105, 106, 165, 221, 456')
    await asyncio.sleep(12)
    
    # convert market data to a dictionary
    mktdicts = {}
    for i, j in md.__dict__.items():
        if not any([x in i for x in ['tick', 'dom', 'auction', 'Event']]): # Filter out attributes not required
            if isinstance(j, Stock): # Split Stock attributes
                for k, l in j.__dict__.items():
                    mktdicts[k] = l
            elif isinstance(j, Index): # Split Stock attributes
                for k, l in j.__dict__.items():
                    mktdicts[k] = l    
            elif isinstance(j, Dividends): # Get dividend attributes
                mktdicts['past12Months'] =j.past12Months
                mktdicts['next12Months'] = j.next12Months
                mktdicts['nextDate'] = j.nextDate
                mktdicts['nextAmount'] = j.nextAmount
            else:
                mktdicts[i] = j
    s1 = pd.Series(mktdicts).dropna() # drop all none
    s1 = s1[s1!=''] # remove all null strings
    s2 = {i: j for i, j in s1.items() if not isinstance(j, list)} # remove combolegs list
    mktdata = {s2['conId']: {k: v for k, v in s2.items() if k !='conId'}}

    return mktdata
   
async def main(ib, contracts):
    """
    Collection of all tasks and awaits to execute
    Args:
        (ib) as connection object
    Returns:
        
    """
    
    price = [asyncio.ensure_future(priceAsync(ib, c)) for c in contracts]
    ohlc = [asyncio.ensure_future(ohlcAsync(ib, c)) for c in contracts]
    mktdata = [asyncio.ensure_future(mktDataAsync(ib, c)) for c in contracts]
    chains = [asyncio.ensure_future(chainsAsync(ib, c)) for c in contracts]
    
    tasks = price + ohlc + mktdata + chains
    
    result = await asyncio.gather(*tasks)
    
    return result

    
if __name__ == "__main__":
    
    import time
    import logging
    from datetime import datetime
    
    logging.getLogger('ib_insync.wrapper').setLevel(logging.FATAL) # supress errors
    logger = logging.getLogger(__name__)
    
    start = datetime.now()
    
    MARKET = 'SNP'

    ROOT_PATH = r'C:/Users/User/Documents/Business/Projects/iboop/'
    FSPATH = ROOT_PATH+r'data/'+MARKET.lower()+'/'

    # Variables initialization
    with open(ROOT_PATH+'var.yml') as f:
        data=yaml.safe_load(f)

    HOST = data["COMMON"]["HOST"]
    PORT = data[MARKET.upper()]["PORT"]
    CID = data["COMMON"]["CID"]
    
    df_unds = pd.read_pickle(FSPATH+'_df_unds.pkl')
    
    # Get some contracts
    
    # contracts = list(df_unds[df_unds.symbol.isin(['MXEF', 'INTC', 'SBUX', 'AAPL', 'ADBE', 'GOOG'])].contract)
    # contracts = list(df_unds[df_unds.symbol.isin(['MXEF'])].contract)
    contracts = list(df_unds.contract)
    
    # .. RTN is a bad egg!
    rtn = [Stock(conId=11758, symbol='RTN', exchange='SMART', 
                primaryExchange='NYSE', currency='USD', 
                localSymbol='RTN', tradingClass='RTN')]
    
    contracts.extend(rtn)
    
    # Start the asyncio loop and get the underlyings
    loop = asyncio.get_event_loop()
    with IB().connect(HOST, PORT, CID) as ib:
        data = loop.run_until_complete(main(ib, contracts))
        
    # Generate the dataframes
    
    # ... ohlcs
    df_ohlcs = pd.concat([v for i in range(len(data)) if data[i] 
                    for k, v in data[i].items() if isinstance(v, pd.DataFrame) 
                    if any(elem in list(v) for elem in ['barCount'])]).set_index('conId')
    
    # ... chains
    df_chains = pd.concat([v for i in range(len(data)) if data[i] 
                    for k, v in data[i].items() if isinstance(v, pd.DataFrame) 
                    if any(elem in list(v) for elem in ['strike'])]).set_index('conId')
    
    # ... undprice
    price_dict = {k: v for i in range(len(data)) if data[i] 
                      for k, v in data[i].items() if isinstance(v, float)}
    df_undPrice = pd.DataFrame([price_dict]).T
    df_undPrice = df_undPrice.rename(columns={0: 'undPrice'})
    
    # ... underlyings
    base_dict = {k: v for i in range(len(data)) if data[i] 
                 for k, v in data[i].items() 
                 if isinstance(v, dict) if not isinstance(list(v.values())[0], float)}
    df_base = pd.DataFrame(base_dict).T
    df_base = df_base.join(df_undPrice)

    # pickle all the data
    with open(FSPATH+'_datadump.pkl', 'wb') as f:
        pickle.dump(data, f)
        
    df_ohlcs.to_pickle(FSPATH + 'df_ohlcs.pkl')
    df_chains.to_pickle(FSPATH + 'df_chains.pkl')
    df_base.to_pickle(FSPATH + 'df_base.pkl')
        
    print(f"\n{len(contracts)} tasks completed @ {time.strftime('%X')} taking {timestr(datetime.now()-start)}\n")

    """ with IB().connect(HOST, PORT, CID) as ib:
        result = ib.run(chainsAsync(ib, contracts[0]))
        print(result) """
