# Generates df with iv, undPrice, etc from inputs as a ``set`` of qualified underlying contracts.

import asyncio
import math

import IPython as ipy
import pandas as pd
import yaml
from ib_insync import *

# Specific to Jupyter. Will be ignored in IDE / command-lines
if ipy.get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
    import nest_asyncio
    nest_asyncio.apply()
    util.startLoop()
    pd.options.display.max_columns = None


async def undCoro(ib: IB, c, FILL_DELAY: int = 18) -> pd.DataFrame:
    '''Coroutine to give underlyings as a dataframe '''

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


async def execute_task_pool(ib: IB, cts: set) -> list:

    CONCURRENTS = 99
    CHECKPOINT = 20
    FILL_DELAY = 20

    tasks = set()
    results = set()

    # initiate remaining cts as the total set of contracts
    remaining = list(cts.copy())

    # do while there is something remaining (or length of remaining cts > 0 )
    while remaining:

        # if len(remaining cts) is greater than concurrents
        if len(remaining) > CONCURRENTS:

            # create first set of concurrent tasks
            tasks = set(asyncio.create_task(undCoro(
                ib=ib, c=c, FILL_DELAY=FILL_DELAY), name=c.symbol) for c in remaining[:CONCURRENTS])

            # do while on the first set of tasks
            while len(tasks):

                # await for first complete
                done, tasks = await asyncio.wait(tasks, timeout=CHECKPOINT, return_when=asyncio.FIRST_COMPLETED)

                # capture the dones
                results.update(done)

                # remove dones from remainings (or decrement length of remaining cts)
                remaining = [r for r in remaining if r.symbol not in [
                    d.get_name() for d in done]]

                """ print({'len of done': len(done),
                       'len of tasks': len(tasks),
                       'len of remaining': len(remaining),
                       'symbols to add': [
                      c.symbol for c in remaining[:len(done)]]}) """

                # create new tasks which is = len of dones
                tasks.update(asyncio.create_task(undCoro(
                    ib=ib, c=c, FILL_DELAY=FILL_DELAY), name=c.symbol) for c in remaining[:len(done)])

                # report remaining contracts
                print(f'\n{len(remaining)} / {len(cts)} contracts remaining...')

        # if len(remaining cts) is less than concurrents, run all concurrently!
        else:

            # create tasks for all the cts
            tasks = set(asyncio.create_task(undCoro(
                ib=ib, c=c, FILL_DELAY=FILL_DELAY), name=c.symbol) for c in remaining)

            # do while on tasks
            while len(tasks):

                # await on checkpoint to report progress
                done, tasks = await asyncio.wait(tasks, timeout=CHECKPOINT)

                # capture the dones
                results.update(done)

                # remove the dones from remaining cts
                remaining = [r for r in remaining if r.symbol not in [
                    d.get_name() for d in done]]

                # report on remaining cts
                print(f'\n{len(remaining)} / {len(cts)} contracts remaining...\n')

    # return the results
    return results

if __name__ == "__main__":

    MARKET = 'SNP'

    # Start the clock
    import time
    start = time.time()

    # Initialize yaml variables
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    HOST = data["COMMON"]["HOST"]
    PORT = data[MARKET.upper()]["PORT"]
    CID = data["COMMON"]["CID"]

    # get the symlots
    df_symlots = pd.read_pickle('./data/' + MARKET.lower() + '_symlots.pkl')

    """ # Run for one contract
    with IB().connect('127.0.0.1', 3000, 0) as ib:
        c = df_symlots.contract.to_list()[5]
        df = ib.run(undCoro(ib, c, 15))
        df.to_pickle('./data/df_unds1_' + MARKET.lower() + '.pkl')
        print(df) """

    # Run for multiple contracts
    cts = set(df_symlots.contract.to_list())
    with IB().connect(HOST, PORT, CID) as ib:
        results = ib.run(execute_task_pool(ib=ib, cts=cts))

    # print(f'Final results: \n{results}')

    df = pd.concat((r.result() for r in results), ignore_index=True)
    df = df.drop_duplicates(subset=['symbol']).reset_index(drop=True)
    df.to_pickle('./data/df_unds2_' + MARKET.lower() + '.pkl')
    print(df[['symbol', 'undPrice', 'impliedVolatility']])

    print('\n')

    print(df[df['undPrice'].isna()])

    print(
        f'\n\n***Time taken: ' +
        f'{time.strftime("%H:%M:%S", time.gmtime(time.time()-start))}\n')
