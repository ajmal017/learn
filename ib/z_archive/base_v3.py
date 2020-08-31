# Generate base using sequential asyncio tasks over a common executor
# cts is made as dictionary with {unique_symbol: raw_contract}, instead of {set}

import asyncio
import math
from collections import defaultdict
from typing import Callable, Coroutine

import IPython as ipy
import numpy as np
import pandas as pd
import yaml
from ib_insync import *

from assemble import assemble
from support import delete_all_files, get_dte

# Specific to Jupyter. Will be ignored in IDE / command-lines
if ipy.get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
    import nest_asyncio
    nest_asyncio.apply()
    util.startLoop()
    pd.options.display.max_columns = None


async def chains(ib: IB, c) -> pd.DataFrame:

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

    return df2


async def ohlcs(ib: IB, c, DURATION: int = 365, OHLC_DELAY: int = 20) -> pd.DataFrame:
    ohlc = await ib.reqHistoricalDataAsync(
        contract=c,
        endDateTime="",
        durationStr=str(DURATION) + ' D',
        barSizeSetting="1 day",
        whatToShow="Trades",
        useRTH=True)
    await asyncio.sleep(OHLC_DELAY)
    df = util.df(ohlc)
    try:
        df.insert(0, 'symbol', c.symbol)
    except AttributeError:
        df = None
    return df


async def optscs(ib: IB, c, **kwargs) -> pd.DataFrame:
    '''Qualifies options. Note: c is a block of options!'''

    cs = await ib.qualifyContractsAsync(*c)

    if cs:
        df = util.df(cs).iloc[:, :6].\
            rename(columns={'lastTradeDateOrContractMonth': 'expiry'})
        df['contract'] = cs
        return df


async def optprices(ib: ib, c, **kwargs) -> pd.DataFrame:

    tick = await ib.reqTickersAsync(*c)

    price = next(x for x in (tick.last, tick.close) if not math.isnan(x))


async def unds(ib, c, **kwargs) -> pd.DataFrame:

    FILL_DELAY = kwargs['FILL_DELAY']

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


async def execute_seq_task_pool(ib: IB,  # An active loop
                                cts: dict,  # Set of contracts
                                MARKET: str,  # Market of the contracts
                                algo: Callable[..., Coroutine],  # coro name
                                CONCURRENTS: int = 99,  # no of concurrencies
                                CHECKPOINT: int = 30,  # checkpoint reporting time
                                SHOW_MSGS: bool = True,  # Suppress internal messages
                                **kwargs,  # inputs for the algo
                                ) -> set:
    '''Executes one algo for all symbols '''

    tasks = set()
    results = set()

    # initialize remaining cts
    # remaining = dict()
    # remaining.update({c.symbol: c for c in cts})
    remaining = cts.copy()

    # do while there is something remaining
    while len(remaining):

        # if len of remaining cts > concurrents, create first set of concurrent tasks
        if len(remaining) >= CONCURRENTS:
            tasks.update(asyncio.create_task(
                algo(ib=ib, c=c, **kwargs), name=k)
                for k, c in dict(list(remaining.items())[:CONCURRENTS]).items())

            # remove these tasks from remaining as they have been put to process!
            for t in tasks:
                del remaining[t.get_name()]

            # do while on the first set of tasks
            while len(tasks):

                # await for first complete and put pending as tasks for the next loop
                done, tasks = await asyncio.wait(tasks,
                                                 timeout=CHECKPOINT,
                                                 return_when=asyncio.FIRST_EXCEPTION)

                # capture the done
                results.update(done)

                # report before creating new tasks
                if SHOW_MSGS:
                    print(
                        f'\npending {len(tasks)} tasks: {sorted([t.get_name() for t in tasks])}')
                    print(
                        f'\ndone {len(done)} tasks: {sorted([d.get_name() for d in done])}\n')

                # create new tasks which is = length of dones
                new_tasks = dict(list(remaining.items())[:len(done)])
                tasks.update(asyncio.create_task(algo(ib=ib, c=c, **kwargs),
                                                 name=k) for k, c in new_tasks.items())

                # delete these new tasks from remaining
                for t in new_tasks.keys():
                    del remaining[t]

                # report status of active tasks
                if SHOW_MSGS or len(remaining) == 0:
                    print(
                        f'..so..{len(new_tasks)} new tasks based on len(done):{len(done)} created: {sorted(new_tasks.keys())}')

                    print(
                        f'\n.....{len(tasks)} tasks still in pipeline: {sorted([t.get_name() for t in tasks])}')

                # report remaining contracts
                print(
                    f'\nremaining: {len(remaining)} out of total: {len(cts)} contracts for {algo.__name__}')

        # if len(remaining cts) is less than concurrents, run all concurrently!
        else:

            tasks.update(asyncio.create_task(
                algo(ib=ib, c=c, **kwargs), name=k)
                for k, c in remaining.items())

            # do while on tasks
            while len(tasks):

                # await on checkpoint to report progress
                done, tasks = await asyncio.wait(tasks, timeout=CHECKPOINT)

                # capture the dones
                results.update(done)

                # remove done from remaining cts
                done_names = [d.get_name() for d in done]
                for d in done_names:
                    print(
                        f'before remaining diff_update done|remaining: {d} || {remaining}')
                    del remaining[d]
                    # remaining.difference_update(d)

                    print(
                        f'after remaining diff_update done|remaining: {d} || {remaining}')

                # report on remaining cts
                print(f'\n{len(remaining)} / {len(cts)} contracts remaining...\n')

    # pickle the results in a df based on MARKET and algo name

    lst = []  # list for storing results

    for r in list(results):
        lst.append(r.result())

    dfr = pd.concat(lst, ignore_index=True)
    dfr.to_pickle(FSPATH + 'df_' + MARKET.lower() +
                  '_' + algo.__name__ + '.pkl')

    # return the results
    # return results, dfr
    return dfr


def prepOpts(BLK_SIZE: int = 100):
    '''Prepare raw options for qualification'''

    # ... read the chains and unds
    df_chains = pd.read_pickle('./data/df_' + MARKET.lower() + '_chains.pkl')
    df_unds = pd.read_pickle('./data/df_' + MARKET.lower() + '_unds.pkl')

    # ... weed out chains within MIN and outside MAX dte
    df_ch = df_chains[df_chains.dte.between(MINDTE, MAXDTE, inclusive=True)
                      ].reset_index(drop=True)

    # ... get the undPrice and volatility
    df_u1 = df_unds[['symbol', 'undPrice', 'impliedVolatility']].rename(
        columns={'impliedVolatility': 'iv'})

    # ... integrate undPrice and volatility to df_ch
    df_ch = df_ch.set_index('symbol').join(
        df_u1.set_index('symbol')).reset_index()

    # ... get the expiries
    if MARKET == 'NSE':
        df_ch['expiryM'] = df_ch.expiry.apply(lambda d: d[:4] + '-' + d[4:6])
        cols1 = ['symbol', 'expiryM']
        df_ch = df_ch.set_index(cols1).join(
            df_symlots[cols1 + ['lot']].set_index(cols1)).reset_index()
        df_ch = df_ch.drop('expiryM', 1)
    else:
        df_ch['lot'] = 100

    # ...compute one standard deviation and mask out chains witin the fence
    df_ch = df_ch.assign(sd1=df_ch.undPrice * df_ch.iv *
                         (df_ch.dte / 365).apply(math.sqrt))

    fence_mask = (df_ch.strike > df_ch.undPrice + df_ch.sd1 * CALLSTD) | \
        (df_ch.strike < df_ch.undPrice - df_ch.sd1 * PUTSTD)

    df_ch1 = df_ch[fence_mask].reset_index(drop=True)

    # ... identify puts and the calls
    df_ch1.insert(3, 'right', np.where(
        df_ch1.strike < df_ch1.undPrice, 'P', 'C'))

    # ... build the options
    opts = [Option(s, e, k, r, x)
            for s, e, k, r, x
            in zip(df_ch1.symbol, df_ch1.expiry, df_ch1.strike,
                   df_ch1.right, ['NSE' if MARKET.upper() == 'NSE' else 'SMART'] * len(df_ch1))]

    # Make singular raw option contracts
    # rawcts = {c.symbol + c.lastTradeDateOrContractMonth +
    #           c.right + str(c.strike): c for c in opts}

    # Make multiple raw option contracts
    raw_blks = [opts[i:i + BLK_SIZE]
                for i in range(0, len(opts), BLK_SIZE)]

    ct_blks = {f'{c[0].symbol + c[0].lastTradeDateOrContractMonth + c[0].right + str(c[0].strike)} to ' +
               f'{c[len(c)-1].symbol + c[len(c)-1].lastTradeDateOrContractMonth + c[len(c)-1].right + str(c[len(c)-1].strike)}': c
               for c in raw_blks}

    return ct_blks


if __name__ == "__main__":

    # Set the market
    MARKET = 'SNP'

    # Start the clock
    import time
    start = time.time()

    # <<< !!! Temporarily commented out >>>
    """ # Delete all data and log files
    delete_all_files(MARKET) """

    # Initialize yaml variables for PATH
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    FSPATH = data[MARKET.upper()]["FSPATH"]
    LOGPATH = data[MARKET.upper()]["LOGPATH"]

    HOST = data["COMMON"]["HOST"]

    PORT = data[MARKET.upper()]["PORT"]
    # PORT = data[MARKET.upper()]["PAPER"]  # For IBG Paper

    CID = data["COMMON"]["CID"]

    MAXDTE = data[MARKET.upper()]["MAXDTE"]
    MINDTE = data[MARKET.upper()]["MINDTE"]
    CALLSTD = data[MARKET.upper()]["CALLSTD"]
    PUTSTD = data[MARKET.upper()]["PUTSTD"]

    # Direct logs to file with level at WARNING (30)
    util.logToFile(path=LOGPATH + 'main.log', level=30)

    # ...clear contents of the log file
    with open(LOGPATH + 'main.log', 'w'):
        pass

    # <<< !!! TEMPORARILY COMMENTED OUT !!! >>>
    """ # Generate symlots
    with IB().connect(HOST, PORT, CID) as ib:
        df_symlots = ib.run(assemble(ib, MARKET, FSPATH)) """

    df_symlots = pd.read_pickle(FSPATH + MARKET.lower() + '_symlots.pkl')

    # Set kwarg inputs for algos
    kws = {'unds': {'FILL_DELAY': 15},
           'chains': {},
           'ohlcs': {'DURATION': 365, 'OHLC_DELAY': 20}}

    # Successful: Experiment with one function (unds) for one contract
    """ cts = set(df_symlots.contract[:2])  # !!! DATA LIMTER

    with IB().connect(HOST, PORT, CID) as ib:
        async def coro():

            # Create task for each contract in a small set of contracts
            tasks = (asyncio.create_task(
                unds(ib, c, **kws['unds']), name=c.symbol) for c in cts)

            return await asyncio.gather(*tasks)

        df_unds = ib.run(coro())

        print(df_unds) """

    # Successful: Experiment with one function (unds) for len(contracts) < CONCURRENT
    """ cts = {c.symbol: c for c in df_symlots.contract[:2]}  # !!! DATA LIMTER

    with IB().connect(HOST, PORT, CID) as ib:
        df_unds = ib.run(execute_seq_task_pool(ib=ib,
                                               cts=cts,
                                               MARKET=MARKET,
                                               algo=unds,
                                               CONCURRENTS=90,
                                               CHECKPOINT=30,
                                               **kws['unds']
                                               ))
        print(df_unds) """

    # Successful: Generating `unds` on ALL symbols. SNP: 60 seconds for 266 contracts!!
    """ cts = set(df_symlots.contract)

    with IB().connect(HOST, PORT, CID) as ib:
        df_unds = ib.run(execute_seq_task_pool(ib=ib,
                                               cts=cts,
                                               MARKET=MARKET,
                                               algo=unds,
                                               CONCURRENTS=100,
                                               CHECKPOINT=15,
                                               **kws['unds']
                                               ))
        print(df_unds) """

    # Successful: Generate ohlcs for a small set of symbols < CONCURRENT
    """ cts = set(df_symlots.contract[:5])

    with IB().connect(HOST, PORT, CID) as ib:
        df_ohlcs = ib.run(execute_seq_task_pool(ib=ib,
                                                cts=cts,
                                                MARKET=MARKET,
                                                algo=ohlcs,
                                                CONCURRENTS=25,
                                                CHECKPOINT=15,
                                                **kws['ohlcs']))

        print(df_ohlcs) """

    # Successful: Generate OHLCs for all symbols
    # cts = set(df_symlots.contract)

    """ with IB().connect(HOST, PORT, CID) as ib:
        df_ohlcs = ib.run(execute_seq_task_pool(ib=ib,
                                                cts=cts,
                                                MARKET=MARKET,
                                                algo=ohlcs,
                                                CONCURRENTS=25,
                                                CHECKPOINT=15,
                                                **kws['ohlcs']))

        print(df_ohlcs) """

    # Successful: Generate chains for all symbols
    """ cts = set(df_symlots.contract)
    with IB().connect(HOST, PORT, CID) as ib:
        df_chains = ib.run(execute_seq_task_pool(ib=ib,
                                                 cts=cts,
                                                 MARKET=MARKET,
                                                 algo=chains,
                                                 CONCURRENTS=100,
                                                 CHECKPOINT=15,
                                                 **kws['chains']))

        print(df_chains) """

    # <<< !!! TEMPORARILY COMMENTED OUT !!! >>>
    """ # Generate all base data
    cts = {c.symbol: c for c in df_symlots.contract}

    with IB().connect(HOST, PORT, CID) as ib:

        # underlyings
        df_unds = ib.run(execute_seq_task_pool(ib=ib,
                                               cts=cts,
                                               MARKET=MARKET,
                                               algo=unds,
                                               CONCURRENTS=100,
                                               CHECKPOINT=15,
                                               **kws['unds']
                                               ))

        # ohlcs
        df_ohlcs = ib.run(execute_seq_task_pool(ib=ib,
                                                cts=cts,
                                                MARKET=MARKET,
                                                algo=ohlcs,
                                                CONCURRENTS=20,
                                                CHECKPOINT=30,
                                                **kws['ohlcs']))

        # chains
        df_chains = ib.run(execute_seq_task_pool(ib=ib,
                                                 cts=cts,
                                                 MARKET=MARKET,
                                                 algo=chains,
                                                 CONCURRENTS=100,
                                                 CHECKPOINT=15,
                                                 **kws['chains'])) 
        # Generate raw options
        ct_blks = prepOpts(BLK_SIZE=200)

        # qualified options
        df_opts = ib.run(execute_seq_task_pool(ib=ib,
                                        cts=ct_blks,
                                        MARKET=MARKET,
                                        algo=optscs,
                                        CONCURRENTS=30,
                                        CHECKPOINT=10,
                                        SHOW_MSGS=False))                                         
                                                 """

    df_opts = pd.read_pickle('./data/df_' + MARKET.lower() + '_optscs.pkl')

    print(
        f'\n\n***Time taken: ' +
        f'{time.strftime("%H:%M:%S", time.gmtime(time.time()-start))}\n')
