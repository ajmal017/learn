# Makes chains and unds
# To be run after assemble

import asyncio
import math

import IPython as ipy
import pandas as pd
import yaml
from ib_insync import *

from assemble import assemble
from support import get_dte

# Specific to Jupyter. Will be ignored in IDE / command-lines
if ipy.get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
    import nest_asyncio
    nest_asyncio.apply()
    util.startLoop()
    pd.options.display.max_columns = None


async def undCoro(ib: IB, c, FILL_DELAY: int = 15) -> pd.DataFrame:

    tick = ib.reqMktData(c, '456, 104, 106, 100, 101, 165', snapshot=False)
    await asyncio.sleep(FILL_DELAY)

    ib.cancelMktData(c)

    try:
        undPrice = next(x for x in (tick.last, tick.close)
                        if not math.isnan(x))
    except Exception as e:
        print(f'undPrice not found in {tick.contract.symbol}. Error: {e}')
        undPrice = None

    m_df = pd.DataFrame(util.df([tick]))
    m_df['undPrice'] = undPrice

    div_df = pd.DataFrame(m_df.dividends.tolist())
    df1 = m_df.drop('dividends', 1).join(div_df)
    df1.insert(0, 'symbol', [c.symbol for c in df1.contract])
    df1['contract'] = c

    df2 = df1.dropna(axis=1)

    # Extract columns with legit values in them
    df3 = df2[[c for c in df2.columns if df2.loc[0, c]]]

    return df3


async def chainsCoro(ib: IB, MARKET: str, c) -> pd.DataFrame:

    chains = await ib.reqSecDefOptParamsAsync(underlyingSymbol=c.symbol,
                                              futFopExchange="",
                                              underlyingSecType=c.secType,
                                              underlyingConId=c.conId)

    # Pick up one chain if it is a list
    chain = chains[0] if isinstance(chains, list) else chains

    df1 = pd.DataFrame([chain])

    # Do a cartesian merge
    df2 = pd.merge(pd.DataFrame(df1.expirations[0], columns=['expiry']).assign(key=1),
                   pd.DataFrame(df1.strikes[0], columns=['strike']).assign(key=1), on='key').\
        merge(df1.assign(key=1)).rename(columns={'tradingClass': 'symbol', 'multiplier': 'mult'})[
        ['symbol', 'expiry', 'strike', 'exchange', 'mult']]

    # Replace tradingclass to reflect correct symbol name of 9 characters
    df2 = df2.assign(symbol=df2.symbol.str[:9])

    # convert & to %26
    df2 = df2.assign(
        symbol=df2.symbol.str.replace("&", "%26"))

    # convert symbols - friendly to IBKR
    df2 = df2.assign(symbol=df2.symbol.str.slice(0, 9))
    ntoi = {"M%26M": "MM", "M%26MFIN": "MMFIN",
            "L%26TFH": "LTFH", "NIFTY": "NIFTY50"}
    df2.symbol = df2.symbol.replace(ntoi)

    # Get the dte
    df2['dte'] = df2.expiry.apply(get_dte)

    """ # Integrate lots
    if MARKET == 'NSE':
        df2['expiryM'] = df2.expiry.apply(lambda d: d[:4] + '-' + d[4:6])
        cols1 = ['symbol', 'expiryM']
        df2 = df2.set_index(cols1).join(
            df_symlots[cols1 + ['lot']].set_index(cols1)).reset_index()
        df2 = df2.drop('expiryM', 1)
    else:
        df2['lot'] = 100 """

    return df2


async def baseCoro(ib):
    '''Creates base tasks and adds it to todo'''

    df_symlots = await asyncio.create_task(assemble(ib, MARKET, FSPATH), name='get_symlots')

    for c in df_symlots.contract:

        todo.add(asyncio.create_task(chainsCoro(
            ib=ib, MARKET=MARKET, c=c), name=c.symbol + '_chain'))

        todo.add(asyncio.create_task(
            undCoro(ib=ib, c=c, FILL_DELAY=25), name=c.symbol + '_und'))


async def do(ib, CHECKPOINT: int = 10):

    global todo, results

    # add baseCoro to todo

    todo.add(asyncio.create_task(baseCoro(ib), name='base_coro'))

    while len(todo):
        done, pending = await asyncio.wait(todo, timeout=CHECKPOINT)

        # report pending progress
        print(f"{len(todo)}: " +
              " ".join(sorted([p.get_name() for p in pending]))[-75:])

        todo.difference_update(done)

        results.update({d.get_name(): d.result() for d in done})

        todo.difference_update(done)


if __name__ == "__main__":

    # Set the market
    MARKET = 'NSE'

    # Initialize yaml variables for PATH
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    FSPATH = data[MARKET.upper()]["FSPATH"]
    LOGPATH = data[MARKET.upper()]["LOGPATH"]

    HOST = data["COMMON"]["HOST"]
    PORT = data[MARKET.upper()]["PORT"]
    CID = data["COMMON"]["CID"]

    # Direct logs to file with level at WARNING (30)
    util.logToFile(path=LOGPATH + 'main.log', level=30)

    # ...clear contents of the log file
    with open(LOGPATH + 'main.log', 'w'):
        pass

    todo = set()
    results = dict()

    with IB().connect(HOST, PORT, CID) as ib:
        ib.run(do(ib, CHECKPOINT=30))

    print(results)
