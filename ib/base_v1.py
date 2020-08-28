# Generates base data

import asyncio
import itertools  # for islicing dictionary
import math
from collections import defaultdict

import IPython as ipy
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


async def chainsCoro(ib: IB, c) -> pd.DataFrame:

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


async def ohlcCoro(ib: IB, c, DURATION: int = 365, OHLC_DELAY: int = 20) -> pd.DataFrame:
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


async def execute_mixed_task_pool(ib: IB,
                                  cts: dict,
                                  CONCURRENTS: int = 99,
                                  CHECKPOINT: int = 30,
                                  FILL_DELAY: int = 20,
                                  OHLC_DELAY: int = 35,
                                  DURATION: int = 365) -> set:
    '''For each symbol runs a mix of unds, ohlcs and chains '''

    tasks = set()
    results = set()

    # initialize remaining cts
    remaining = defaultdict(dict)
    remaining.update(cts)

    # print(remaining) # temp

    def add_tasks(n):
        '''adds tasks to the loop'''

        d = dict(itertools.islice(remaining.items(), 0, n))

        for k, v in d.items():

            # print(k.split('_')[1]) # temp

            if k.split('_')[1] == 'unds':
                tasks.add(asyncio.create_task(
                    undCoro(ib=ib, c=v, FILL_DELAY=FILL_DELAY),
                    name=k))

            if k.split('_')[1] == 'ohlcs':
                tasks.add(asyncio.create_task(
                    ohlcCoro(ib=ib, c=v, DURATION=DURATION,
                             OHLC_DELAY=OHLC_DELAY),
                    name=k))

            if k.split('_')[1] == 'chains':
                tasks.add(asyncio.create_task(
                    chainsCoro(ib=ib, c=v),
                    name=k))

        return None

    # do while there is something remaining
    while len(remaining):

        # if len of remaining cts > concurrents, create first set of concurrent tasks
        if len(remaining) > CONCURRENTS:

            add_tasks(CONCURRENTS)

            # do while on the first set of tasks
            while len(tasks):

                # await for first complete
                done, tasks = await asyncio.wait(tasks,
                                                 timeout=CHECKPOINT,
                                                 return_when=asyncio.FIRST_COMPLETED)

                # capture the done
                results.update(done)

                # remove done from remaining cts
                done_names = [d.get_name() for d in done]
                for d in done_names:
                    print(f'{d} || {remaining[d]}')
                    del remaining[d]

                # create new tasks which is = length of dones
                add_tasks(len(done))

                # report remaining contracts
                print(f'\n{len(remaining)} / {len(cts)} contracts remaining...')

        # if len(remaining cts) is less than concurrents, run all concurrently!
        else:

            add_tasks(len(remaining))

            # do while on tasks
            while len(tasks):

                # await on checkpoint to report progress
                done, tasks = await asyncio.wait(tasks, timeout=CHECKPOINT)

                # capture the dones
                results.update(done)

                # remove done from remaining cts
                done_names = [d.get_name() for d in done]
                for d in done_names:
                    del remaining[d]

                # report on remaining cts
                print(f'\n{len(remaining)} / {len(cts)} contracts remaining...\n')

    # return the results
    return results


def process_base_result(results: dict, MARKET: str):

    ohlcs = []
    unds = []
    chains = []
    basetype = []

    for v in list(results):
        try:
            basetype = v.get_name().split('_')[1]

        except IndexError as e:
            print(f"{v.get_name()} is not a valid base df")

        # try:
        if basetype == 'ohlcs':
            ohlcs.append(v.result())
        if basetype == 'unds':
            unds.append(v.result())
        if basetype == 'chains':
            chains.append(v.result())

    # build the dataframes and pickle
    if unds:
        df_unds = pd.concat(unds, ignore_index=True)
        df_unds = df_unds[~df_unds.time.isnull()].drop_duplicates(
            subset=['symbol'])
        df_unds.to_pickle(FSPATH + 'df_unds_' + MARKET.lower() + '.pkl')

    if ohlcs:
        df_ohlcs = pd.concat(ohlcs, ignore_index=True)
        df_ohlcs = df_ohlcs.drop_duplicates(
            subset=['symbol', 'date'])  # remove duplicated ohlcs
        df_ohlcs.to_pickle(
            FSPATH + 'df_ohlcs_' + MARKET.lower() + '.pkl')

    if chains:
        df_chains = pd.concat(chains, ignore_index=True)
        df_chains = df_chains.drop_duplicates(
            subset=['symbol', 'expiry', 'strike'])
        df_chains.to_pickle(
            FSPATH + 'df_chains_' + MARKET.lower() + '.pkl')


if __name__ == "__main__":

    # Set the market
    MARKET = 'SNP'

    # Start the clock
    import time
    start = time.time()

    # <<< !!! Temporarily commented out >>>
    # # Delete all data and log files
    # delete_all_files(MARKET)

    # Initialize yaml variables for PATH
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    FSPATH = data[MARKET.upper()]["FSPATH"]
    LOGPATH = data[MARKET.upper()]["LOGPATH"]

    HOST = data["COMMON"]["HOST"]

    PORT = data[MARKET.upper()]["PORT"]
    PORT = data[MARKET.upper()]["PAPER"]  # For IBG Paper

    CID = data["COMMON"]["CID"]

    # Direct logs to file with level at WARNING (30)
    util.logToFile(path=LOGPATH + 'main.log', level=30)

    # ...clear contents of the log file
    with open(LOGPATH + 'main.log', 'w'):
        pass

    # <<< !!! Temporarily commented out >>>
    """ # Generate symlots
    with IB().connect(HOST, PORT, CID) as ib:
        df_symlots = ib.run(assemble(ib, MARKET, FSPATH)) """

    df_symlots = pd.read_pickle(FSPATH + MARKET.lower() + '_symlots.pkl')

    # Stage inputs: contracts as a dictionary for each coro
    cts = dict()
    for c in df_symlots.contract:
        cts.update({c.symbol + '_unds': c,
                    c.symbol + '_ohlcs': c,
                    c.symbol + '_chains': c})

    # Mixed unds+ohlc+chains function run for each symbol
    # ...takes 2.5 hours for SNP!
    # Create task pool and execute the tasks
    with IB().connect(HOST, PORT, CID) as ib:
        results = ib.run(execute_mixed_task_pool(ib=ib, cts=cts))

    # Segregate the results
    process_base_result(results=results, MARKET=MARKET)

    print(
        f'\n\n***Time taken: ' +
        f'{time.strftime("%H:%M:%S", time.gmtime(time.time()-start))}\n')
