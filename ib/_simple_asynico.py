# Breaking down a working asyncio + IB on .py
# With an external todo

# Base case: qualify option chains

import asyncio
import math

import numpy as np
import pandas as pd
import yaml
from ib_insync import IB, Option

if __name__ == "__main__":

    MARKET = 'SNP'

    # initialize yaml variables
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    HOST = data["COMMON"]["HOST"]
    PORT = data[MARKET.upper()]["PORT"]
    CID = data["COMMON"]["CID"]
    MASTERCID = data["COMMON"]["MASTERCID"]

    FSPATH = data[MARKET.upper()]["FSPATH"]

    MAXDTE = data[MARKET.upper()]["MAXDTE"]
    MINDTE = data[MARKET.upper()]["MINDTE"]
    CALLSTD = data[MARKET.upper()]["CALLSTD"]
    PUTSTD = data[MARKET.upper()]["PUTSTD"]

    # Make a raw opts contract list that are dte and stdev filtered

    df_chains = pd.read_pickle(FSPATH + 'df_chains.pkl')
    df_unds = pd.read_pickle(FSPATH + 'df_unds.pkl')

    # ..get the chains

    df_ch = df_chains[df_chains.dte.between(MINDTE, MAXDTE, inclusive=True)
                      ].reset_index(drop=True)

    # ..integrate with iv and undPrice
    df_ch = df_ch.set_index('symbol').join(
        df_unds.set_index('symbol')[['impliedVolatility', 'undPrice', 'contract']]).reset_index()

    df_ch.rename(columns={'impliedVolatility': 'iv'}, inplace=True)

    # ..compute 1 stdev and mask chains within fence
    df_ch = df_ch.assign(sd1=df_ch.undPrice * df_ch.iv * (df_ch.dte / 365).
                         apply(math.sqrt))

    fence_mask = (df_ch.strike > df_ch.undPrice + df_ch.sd1 * CALLSTD) | \
                 (df_ch.strike < df_ch.undPrice - df_ch.sd1 * PUTSTD)

    df_ch = df_ch[fence_mask].reset_index(drop=True)

    # ..identify puts and calls
    df_ch.insert(3, 'right', np.where(df_ch.strike < df_ch.undPrice, 'P', 'C'))

    # .. make the opts raw contract list
    opts = [Option(s, e, k, r, x)
            for s, e, k, r, x
            in zip(df_ch.symbol, df_ch.expiry,
                   df_ch.strike, df_ch.right, ['NSE'
                                               if MARKET.upper() == 'NSE' else 'SMART'] *
                   len(df_ch))]

    # Qualify the options

    BLK = 100

    optblks = [opts[i: i + BLK] for i in range(0, len(opts), BLK)]

    blkdict = dict()
    todo = set()
    result = set()

    for optblk in optblks:

        begins = optblk[0]
        ends = optblk[len(optblk) - 1]
        nm = begins.symbol + str(begins.strike) +\
            begins.right + begins.lastTradeDateOrContractMonth + '|' +\
            ends.symbol + str(ends.strike) +\
            ends.right + ends.lastTradeDateOrContractMonth
        blkdict.update({nm: optblk})

    async def qualCoro(blkdict):

        for k, v in blkdict.items():
            todo.add(asyncio.create_task(
                ib.qualifyContractsAsync(*v), name=k))

    async def main():
        '''Main routine: Makes task out of necessary tasks.
           Adds it to running loop.
           Monitors progress using asyncio.wait
        '''

        # add task to todo
        todo.add(asyncio.create_task(
            qualCoro(blkdict), name='qualCoro'))

        while len(todo):
            done, pending = await asyncio.wait(todo, timeout=5)
            todo.difference_update(done)
            result.add(d.result() for d in done if d)

    # Single entry point for IB() to run the main routine
    with IB().connect(HOST, PORT, CID) as ib:
        ib.run(main())

    # Unpacks the ``generated`` result
    qopts = *(i for j in result for i in j)

    print(qopts)
