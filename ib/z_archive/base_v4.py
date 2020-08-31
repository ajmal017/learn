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
from support import calcsd_df, delete_all_files, get_dte

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


async def optscs(ib: IB, c, **kwargs) -> pd.DataFrame:
    '''Qualifies options. Note: c is a block of options!'''

    cs = await ib.qualifyContractsAsync(*c)

    if cs:
        df = util.df(cs).iloc[:, :6].\
            rename(columns={'lastTradeDateOrContractMonth': 'expiry'})
        df['contract'] = cs
        return df


async def prices(ib: IB, c, **kwargs) -> pd.DataFrame:
    '''Gets prices in blocks'''

    ticks = await ib.reqTickersAsync(*c)

    try:
        dfpr = util.df(ticks)

    except AttributeError:

        # return an empty price dataframe
        dfpr = pd.DataFrame({'localSymbol': {}, 'conId': {}, 'contract': {},
                             'undPrice': {}, 'bid': {}, 'ask': {}, 'close': {},
                             'last': {}, 'price': {}, 'iv': {}})

        return dfpr

    iv = dfpr.modelGreeks.apply(lambda x: x.impliedVol if x else None)
    undPrice = dfpr.modelGreeks.apply(lambda x: x.undPrice if x else None)
    price = dfpr['close'].combine_first(dfpr['last'])
    conId = dfpr.contract.apply(lambda x: x.conId if x else None)
    symbol = dfpr.contract.apply(lambda x: x.symbol if x else None)
    strike = dfpr.contract.apply(lambda x: x.strike if x else None)
    right = dfpr.contract.apply(lambda x: x.right if x else None)
    expiry = dfpr.contract.apply(
        lambda x: x.lastTradeDateOrContractMonth if x else None)
    # localSymbol = dfpr.contract.apply(lambda x: x.localSymbol if x else None)

    dfpr = dfpr.assign(conId=conId, symbol=symbol, strike=strike, right=right, expiry=expiry, iv=iv,
                       undPrice=undPrice, price=price)

    cols = ['symbol', 'conId', 'strike', 'undPrice', 'expiry', 'right', 'contract', 'bid',
            'ask', 'close', 'last', 'price', 'iv']
    dfpr = dfpr[cols]

    return dfpr


async def margins(ib: IB, c, **kwargs) -> pd.DataFrame:
    '''Gets margins in blocks. c is a block of multiple (c, o)s '''

    async def wifAsync(ct, o):
        wif = ib.whatIfOrderAsync(ct, o)
        await asyncio.sleep(0)
        return wif

    tsk = [await wifAsync(ct, o) for ct, o in c]

    wifs = await asyncio.gather(*tsk)

    try:
        df_wifs = util.df(wifs)[['initMarginChange', 'maxCommission',
                                 'commission']]
    except Exception:
        df_wifs = pd.DataFrame(
            {'initMarginChange': None, 'maxCommission': None,
             'commission': None}, index=range(len(c)))

    df_opts = util.df([ct for ct, o in c]).iloc[:, :6]

    df_mgns = pd.concat([df_opts, df_wifs], axis=1).\
        rename(columns={'lastTradeDateOrContractMonth': 'expiry',
                        'initMarginChange': 'margin'})

    # clean up commission, make margin as float and introduce lots
    lot = [o.totalQuantity if ct.exchange == 'NSE' else 100 for ct, o in cos]

    df_mgns = df_mgns.assign(lot=lot,
                             comm=df_mgns[['commission', 'maxCommission']].min(
                                 axis=1),
                             margin=df_mgns.margin.astype('float')).drop(['commission', 'maxCommission'], axis=1)

    return df_mgns


async def execute_seq_task_pool(ib: IB,  # An active loop
                                # Dictionary of contracts with {name: list}
                                cts: dict,
                                MARKET: str,  # Market of the contracts
                                algo: Callable[..., Coroutine],  # coro name
                                CONCURRENTS: int = 99,  # no of concurrencies
                                CHECKPOINT: int = 30,  # checkpoint reporting time
                                SHOW_MSGS: bool = True,  # Suppress internal messages
                                # Output file name. Ignore pickling if absent.
                                OUTPUT_FILE: str = '',
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

            last_len_tasks = 0  # tracking last length for error catch

            # do while on the first set of tasks
            while len(tasks):

                # await for first complete and put pending as tasks for the next loop
                done, tasks = await asyncio.wait(tasks,
                                                 timeout=CHECKPOINT,
                                                 return_when=asyncio.ALL_COMPLETED)

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

                    if len(tasks) == last_len_tasks:  # something is wrong in tasks
                        print(f'\n !!! WARNING: tasks not progressing. Remaining taks will be skipped.\n' +
                              f'...delete {[t.get_name() for t in tasks]} contract(s) (or) increase checkpoint timeout...\n')
                        dn, pend = await asyncio.wait(tasks, timeout=1.0)
                        tasks.difference_update(dn)
                        tasks.difference_update(pend)

                    # re-initialize last length of tasks
                    last_len_tasks = len(tasks)

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
                    if SHOW_MSGS:
                        print(
                            f'before remaining diff_update done|remaining: {d} || {remaining}')
                    del remaining[d]
                    # remaining.difference_update(d)
                    if SHOW_MSGS:
                        print(
                            f'after remaining diff_update done|remaining: {d} || {remaining}')

                # report on remaining cts
                print(
                    f'\n{len(remaining)} / {len(cts)} contracts remaining on {algo.__name__}...\n')

    # pickle the results in a df based on MARKET and algo name

    lst = []  # list for storing results

    for r in list(results):
        lst.append(r.result())

    dfr = pd.concat(lst, ignore_index=True)

    if OUTPUT_FILE:
        dfr.to_pickle(FSPATH + 'df_' + MARKET.lower() +
                      '_' + OUTPUT_FILE + '.pkl')

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
    MARKET = 'NSE'

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

    # Generate all base data
    cts = {c.symbol: c for c in df_symlots.contract}

    with IB().connect(HOST, PORT, CID) as ib:

        # underlyings
        """ df_unds = ib.run(execute_seq_task_pool(ib=ib,
                                               cts=cts,
                                               MARKET=MARKET,
                                               algo=unds,
                                               CONCURRENTS=100,
                                               CHECKPOINT=15,
                                               OUTPUT_FILE='unds',
                                               **kws['unds']
                                               ))

        # ohlcs
        df_ohlcs = ib.run(execute_seq_task_pool(ib=ib,
                                                cts=cts,
                                                MARKET=MARKET,
                                                algo=ohlcs,
                                                CONCURRENTS=20,
                                                CHECKPOINT=30,
                                                OUTPUT_FILE='ohlcs',
                                                **kws['ohlcs']))

        # chains
        df_chains = ib.run(execute_seq_task_pool(ib=ib,
                                                 cts=cts,
                                                 MARKET=MARKET,
                                                 algo=chains,
                                                 CONCURRENTS=100,
                                                 CHECKPOINT=15,
                                                 OUTPUT_FILE='chains',
                                                 ** kws['chains']))
        # Generate raw options in blocks
        ct_blks = prepOpts(BLK_SIZE=200)

        # qualified options
        df_opts = ib.run(execute_seq_task_pool(ib=ib,
                                               cts=ct_blks,
                                               MARKET=MARKET,
                                               algo=optscs,
                                               CONCURRENTS=30,
                                               CHECKPOINT=10,
                                               OUTPUT_FILE='qopts',
                                               SHOW_MSGS=False)) """

        df_opts = pd.read_pickle('./data/df_' + MARKET.lower() + '_qopts.pkl')

        # Make qualified option contract blocks for prices
        BLK_SIZE = 100
        opts = df_opts.contract.to_list()
        raw_blks = [opts[i:i + BLK_SIZE]
                    for i in range(0, len(opts), BLK_SIZE)]

        opt_blks = {f'{c[0].localSymbol} to ' + f'{c[len(c)-1].localSymbol}': c
                    for c in raw_blks}

        # prices
        df_prices = ib.run(execute_seq_task_pool(ib=ib,
                                                 cts=opt_blks,
                                                 MARKET=MARKET,
                                                 algo=prices,
                                                 CONCURRENTS=50,
                                                 CHECKPOINT=15,
                                                 OUTPUT_FILE='opt_prices',
                                                 SHOW_MSGS=False))

        # Make options data ready for margins
        # ... determine the lots to set up for margins
        if MARKET == 'NSE':
            df_opts['expiryM'] = df_opts.expiry.apply(
                lambda d: d[:4] + '-' + d[4:6])
            cols1 = ['symbol', 'expiryM']
            df_opts = df_opts.set_index(cols1).join(
                df_symlots[cols1 + ['lot']].set_index(cols1)).reset_index()
            df_opts = df_opts.drop('expiryM', 1)
        else:
            df_opts['lot'] = 100

        # ... build cos (contract, orders)
        opts = df_opts.contract.to_list()
        orders = [MarketOrder('SELL', lot / lot) if MARKET.upper() ==
                  'SNP' else MarketOrder('SELL', lot) for lot in df_opts.lot]
        cos = [(c, o) for c, o in zip(opts, orders)]

        # cos = cos[:10]  # !!! DATA LIMITER !!!

        BLK_SIZE = 25

        co_blks = [cos[i: i + BLK_SIZE] for i in range(0, len(cos), BLK_SIZE)]

        co_dict = {f'{co[0][0].localSymbol} to {co[len(co)-1][0].localSymbol}': co
                   for co in co_blks}

        df_margins = ib.run(execute_seq_task_pool(ib=ib,
                                                  cts=co_dict,
                                                  MARKET=MARKET,
                                                  algo=margins,
                                                  CONCURRENTS=10,
                                                  CHECKPOINT=10,
                                                  OUTPUT_FILE='opt_margins',
                                                  SHOW_MSGS=False))

    print(
        f'\n\n***Time taken: ' +
        f'{time.strftime("%H:%M:%S", time.gmtime(time.time()-start))}\n')
