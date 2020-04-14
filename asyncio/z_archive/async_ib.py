import asyncio

import numpy as np
import pandas as pd
import tqdm
import yaml
from ib_insync import IB, Stock, util


# Handle errors
def onErrorEvent(reqId, errorCode, errorString, contract):
    print(f"{contract.symbol} error on reqId:{reqId} : {errorString}")
    return None

# A single async function
async def ohlcAsync(ib, c):
    """Args:
        (ib) as connection object
        (c) as a single contract
    Returns:
        df_ohlc as a DataFrame object"""
    
    # handle errors
    try:  
        ohlc = await ib.reqHistoricalDataAsync(
            contract=c,
            endDateTime="",
            durationStr="5 D",
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

# A 2 ohlcs function!!
def get_2_ohlcs(ib, c):
    """Returns two DataFrames concatenated"""

    tasks = asyncio.gather(*[ohlcAsync(ib, c) for c in contracts])
    
    # ohlcs = await tasks # to be replaced in .py file
    ohlcs = ib.run(tasks)

    df = pd.concat(ohlcs)

    return df
    
async def coros(ib, contracts):
    tasks = asyncio.gather(*[ohlcAsync(ib, c) for c in contracts])
    return await tasks

async def main():
    with await IB().connectAsync(host=HOST, port=PORT, clientId=CID) as ib:
        oh_list = await coros(ib, contracts)
        return oh_list

if __name__ == "__main__":
    
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
    
    """ #### Testing get_2_ohlc(). Successful!!
    
    # Make two contracts
    contracts = df_unds.groupby('ctype').head(1).contract

    with IB().connect(host=HOST, port=PORT, clientId=CID) as ib:
        oh_list = [get_2_ohlcs(ib, c) for c in contracts]
        
    df_ohlcs = pd.concat(oh_list)
    print(df_ohlcs) """
    
    #### Testing tqdm with a bad egg.
    
    # Get some contracts
    contracts = list(df_unds[df_unds.symbol.isin(['INTC', 'SBUX'])].contract)
    
    # RTN is a bad egg!
    rtn = [Stock(conId=11758, symbol='RTN', exchange='SMART', 
                primaryExchange='NYSE', currency='USD', 
                localSymbol='RTN', tradingClass='RTN')]
    contracts.extend(rtn)
    
    print(asyncio.run(main()))
