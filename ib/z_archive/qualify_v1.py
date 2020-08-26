# Program to assemble base data with asyncio

import asyncio
from ib_insync import Stock, Index, IB, util
from typing import TypeVar
import time

from ib01_getsyms import get_syms

MARKET = 'SNP'

HOST = '127.0.0.1'
PORT = 4002 if MARKET.upper() == 'NSE' else 4002  # Paper trades!!
CID = 0

# Suppress errors in console
logfile = './data/test.log'
with open(logfile, 'w'):
    pass
util.logToFile(logfile, level=30)

# Get the symbols
df_syms = get_syms(MARKET)

# Build the contracts
raw_cts = [i for j in [[Stock(symbol, exchange, currency), Index(symbol, exchange, currency)]
                       for symbol, exchange, currency
                       in zip(df_syms.symbol, df_syms.exchange, df_syms.currency)] for i in j]


def qual():
    '''Simple qualify, without async'''
    start = time.time()
    result = ib.qualifyContracts(*cts)
    qcon = {r.symbol: r for r in result if r}

    print(
        f'\ntook qual {time.time()-start} seconds to qualify {len(qcon)} contracts\n')
    print(qcon)

    return qcon


async def qual_coro1():
    '''With create_task for every contract'''

    start = time.time()

    tasks = [asyncio.create_task(
        ib.qualifyContractsAsync(s), name=s.symbol) for s in cts]

    result = dict()

    while tasks:
        done, pending = await asyncio.wait(tasks, timeout=2, return_when=asyncio.ALL_COMPLETED)
        sym_con = {d.get_name(): d.result()[0]
                   for d in done if d.result()}
        result.update(sym_con)
        tasks = pending

    print(
        f'\ntook coro1 {time.time()-start} seconds to qualify {len(result)} contracts\n')
    print(result)

    return result


async def qual_coro2():
    '''Without create_task for every contract'''

    start = time.time()

    tasks = [ib.qualifyContractsAsync(s) for s in cts]

    result = {}

    while tasks:
        done, pending = await asyncio.wait(tasks, timeout=2, return_when=asyncio.ALL_COMPLETED)
        sym_con = {d.result()[0].symbol: d.result()[0]
                   for d in done if d.result()}
        result.update(sym_con)
        tasks = pending

    print(
        f'\ntook coro2 {time.time()-start} seconds to qualify {len(result)} contracts\n')
    print(result)

    return result


with IB().connect(HOST, PORT, CID) as ib:
    cts = raw_cts
    result = qual()
    result1 = ib.run(qual_coro1())
    result2 = ib.run(qual_coro2())
