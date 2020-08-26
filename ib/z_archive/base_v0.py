import asyncio
import pickle
import sys
from collections import defaultdict
from datetime import datetime
from pprint import pprint

from ib_insync import IB, util

# added to prevent Event Loop exception...
if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# time formatter for logs


def timestr(end):
    """Gets string for time in h:m:s

    Arg:
        end as timedelta
    Returns:
        xh: ym: zs as string"""

    sec = end.seconds
    hours = sec // 3600
    minutes = (sec // 60) - (hours * 60)
    secs = sec - (minutes * 60) - (hours * 3600)

    return str(hours) + "h: " + str(minutes) + "m: " + str(secs) + "s"


MARKET = 'SNP'

HOST = '127.0.0.1'
PORT = 4002 if MARKET.upper() == 'NSE' else 4002  # Paper trades!!
CID = 0

DATAPATH = './data/'
LONG_BAR_FORMAT = '{desc:<25}{percentage:3.0f}%|{bar:30}{r_bar}'

start = datetime.now()

# Suppress errors in console
logfile = './data/test.log'
with open(logfile, 'w'):
    pass

util.logToFile(logfile, level=30)

# get the underlying contracts
with open(DATAPATH + 'und_cts.pkl', 'rb') as f:
    qunds = pickle.load(f)

und_cts = qunds.values()


# Define the base async defs
async def ohlcCoro(c, DURATION=365, SLEEP=5):
    ohlc = ib.reqHistoricalDataAsync(
        contract=c,
        endDateTime="",
        durationStr=str(DURATION) + ' D',
        barSizeSetting="1 day",
        whatToShow="Trades",
        useRTH=True)

    # Refer: https://stackoverflow.com/a/40572169/7978112
    await asyncio.sleep(SLEEP)

    return ohlc


async def mktdataCoro(c, SLEEP=3):

    tick = ib.reqMktData(
        c, '456, 104, 106, 100, 101, 165')

    await asyncio.sleep(SLEEP)

    ib.cancelMktData(c)

    return tick


async def chainsCoro(c, SLEEP=2):
    chains = ib.reqSecDefOptParamsAsync(
        underlyingSymbol=c.symbol, futFopExchange="",
        underlyingSecType=c.secType, underlyingConId=c.conId)

    await asyncio.sleep(SLEEP)

    return chains


async def baseCoro(c, SLEEP=8):
    tasks = [asyncio.create_task(ohlcCoro(c, DURATION=365, SLEEP=SLEEP), name='ohlc'),
             asyncio.create_task(mktdataCoro(
                 c, SLEEP=SLEEP), name='mdata'),
             asyncio.create_task(chainsCoro(c, SLEEP=2), name='chains')]

    await asyncio.sleep(SLEEP)

    return await asyncio.gather(*tasks)


async def async_main(cts) -> None:

    tasks = [asyncio.create_task(baseCoro(c), name=c.symbol) for c in cts]

    result = defaultdict(list)

    while True:

        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.ALL_COMPLETED)

        if done:
            for d in done:
                for x in d.result():
                    result.update({d.get_name(): d.result()})

        if len(pending) == 0:
            break

        done = pending

    return result

cts = list(und_cts)[:8]


# Test ohlc coro only
if __name__ == "__main__":

    async def get_ohlcs(cts):
        tasks = [asyncio.create_task(
            ohlcCoro(c, DURATION=5, SLEEP=5), name=c.symbol) for c in cts]

        result = defaultdict(dict)
        for coro in asyncio.as_completed(tasks, timeout=None):
            result.update({coro: await coro})

        return result

    with IB().connect(HOST, PORT, CID) as ib:
        result = ib.run(get_ohlcs(cts))

    pprint(result)
    print('\n')
    print(f"Took {timestr(datetime.now() - start)}")


""" with open(DATAPATH + 'result.pkl', 'wb') as f:
    pickle.dump(result, f) """
