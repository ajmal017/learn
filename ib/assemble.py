# Assembles underlying symbols with lots and qualified contracts

import asyncio
from io import StringIO

import numpy as np
import pandas as pd
import requests
import yaml
from ib_insync import IB, Index, Stock, util


async def qSymsCoro(ib: IB, df_symlots: pd.DataFrame) -> pd.DataFrame:
    '''Qualify underlying symbols and get its contracts'''

    dfs = df_symlots.copy()
    df = dfs.drop_duplicates(subset='symbol')

    raw_cts = dict()
    tasks = set()

    raw_cts = {row.symbol: Stock(symbol=row.symbol, exchange=row.exchange, currency=row.currency)
               if row.ctype == 'Stock'
               else Index(symbol=row.symbol, exchange=row.exchange, currency=row.currency) for _, row in df.iterrows()}

    tasks.update(await ib.qualifyContractsAsync(*raw_cts.values()))

    dfs['contract'] = dfs.symbol.map({t.symbol: t for t in tasks})

    dfs = dfs.dropna(subset=['contract']).reset_index(drop=True)

    dfs = dfs.drop(['ctype', 'exchange', 'currency'], 1)

    dfs.insert(1, 'undId', [c.conId for c in dfs.contract])

    return dfs


async def get_nse(ib: IB) -> pd.DataFrame:
    '''Make nse symbols, qualify them and put into a dataframe'''

    url = "https://www1.nseindia.com/content/fo/fo_mktlots.csv"

    try:
        req = requests.get(url)
        if req.status_code == 404:
            print(f'\n{url} URL contents not correct. 404 error!!!')
        df_symlots = pd.read_csv(StringIO(req.text))
    except requests.ConnectionError as e:
        print(f'Connection Error {e}')
    except pd.errors.ParserError as e:
        print(f'Parser Error {e}')

    df_symlots = df_symlots[list(df_symlots)[1:5]]

    # strip whitespace from columns and make it lower case
    df_symlots.columns = df_symlots.columns.str.strip().str.lower()

    # strip all string contents of whitespaces
    df_symlots = df_symlots.applymap(
        lambda x: x.strip() if type(x) is str else x)

    # remove 'Symbol' row
    df_symlots = df_symlots[df_symlots.symbol != "Symbol"]

    # melt the expiries into rows
    df_symlots = df_symlots.melt(
        id_vars=["symbol"], var_name="expiryM", value_name="lot"
    ).dropna()

    # remove rows without lots
    df_symlots = df_symlots[~(df_symlots.lot == "")]

    # convert expiry to period
    df_symlots = df_symlots.assign(
        expiryM=pd.to_datetime(df_symlots.expiryM, format="%b-%y")
        .dt.to_period("M")
        .astype("str")
    )

    # convert lots to integers
    df_symlots = df_symlots.assign(
        lot=pd.to_numeric(df_symlots.lot, errors="coerce"))

    # convert & to %26
    df_symlots = df_symlots.assign(
        symbol=df_symlots.symbol.str.replace("&", "%26"))

    # convert symbols - friendly to IBKR
    df_symlots = df_symlots.assign(symbol=df_symlots.symbol.str.slice(0, 9))
    ntoi = {"M%26M": "MM", "M%26MFIN": "MMFIN",
            "L%26TFH": "LTFH", "NIFTY": "NIFTY50"}
    df_symlots.symbol = df_symlots.symbol.replace(ntoi)

    # differentiate between index and stock
    df_symlots.insert(
        1, "ctype", np.where(
            df_symlots.symbol.str.contains("NIFTY"), "Index", "Stock")
    )

    df_symlots['exchange'] = 'NSE'
    df_symlots['currency'] = 'INR'

    # Get the contracts
    df = await qSymsCoro(ib, df_symlots)

    return df


async def get_snp(ib: IB) -> pd.DataFrame():
    '''Generate symlots for SNP 500 weeklies + those in portfolio as a DataFrame'''

    # Get the weeklies
    dls = "http://www.cboe.com/products/weeklys-options/available-weeklys"

    try:
        data = pd.read_html(dls)
    except Exception as e:
        print(f'Error: {e}')

    df_ix = pd.concat([data[i].loc[:, :0] for i in range(1, 3)], ignore_index=True)\
        .rename(columns={0: 'symbol'})
    df_ix = df_ix[df_ix.symbol.apply(len) <= 5]
    df_ix['ctype'] = 'Index'
    df_ix['exchange'] = 'CBOE'

    df_eq = data[4].iloc[:, :1].rename(columns={0: 'symbol'})
    df_eq = df_eq[df_eq.symbol.apply(len) <= 5]
    df_eq['ctype'] = 'Stock'
    df_eq['exchange'] = 'SMART'

    df_weeklies = pd.concat([df_ix, df_eq], ignore_index=True)
    df_weeklies = df_weeklies.assign(
        symbol=df_weeklies.symbol.str.replace('[^a-zA-Z]', ''))

    # Generate the snp 500s
    try:
        s500 = list(pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
                                 header=0, match='Symbol')[0].loc[:, 'Symbol'])
    except Exception as e:
        print(f'Error: {e}')

    # without dot in symbol
    snp500 = [s.replace('.', '') if '.' in s else s for s in s500]

    # Keep only equity weeklies that are in S&P 500, and all indexes in the weeklies
    df_symlots = df_weeklies[((df_weeklies.ctype == 'Stock') & (df_weeklies.symbol.isin(snp500))) |
                             (df_weeklies.ctype == 'Index')].reset_index(drop=True)

    pf = ib.portfolio()

    more_syms = {p.contract.symbol for p in pf} - set(df_symlots.symbol)

    df_syms = pd.concat([pd.DataFrame({'symbol': list(more_syms), 'ctype': 'Stock', 'exchange': 'SMART'}),
                         pd.DataFrame({'symbol': list(more_syms),
                                       'ctype': 'Index', 'exchange': 'SMART'}),
                         df_symlots], ignore_index=False)

    # Append other symlot fields
    df_syms = df_syms.assign(
        expiry=None, lot=100, currency='USD').drop_duplicates().reset_index(drop=True)

    # Get the contracts
    df = await qSymsCoro(ib, df_syms)

    df = df.drop_duplicates()

    return df


async def assemble(ib, MARKET, FSPATH):

    # run the assemble program
    tsk = asyncio.create_task(get_snp(ib)) if MARKET.upper(
    ) == 'SNP' else asyncio.create_task(get_nse(ib))

    df = await tsk

    df.to_pickle(FSPATH + MARKET.lower() + '_symlots.pkl')

    return df


if __name__ == "__main__":

    MARKET = 'SNP'

    import time

    start = time.time()

    # Initialize yaml variables
    with open('var.yml') as f:
        data = yaml.safe_load(f)

    FSPATH = data[MARKET.upper()]["FSPATH"]
    LOGPATH = data[MARKET.upper()]["LOGPATH"]

    HOST = data["COMMON"]["HOST"]
    PORT = data[MARKET.upper()]["PORT"]
    CID = data["COMMON"]["CID"]

    # Direct logs to file with level at WARNING (30)
    util.logToFile(path=LOGPATH + 'assemble.log', level=30)

    # ...clear contents of the log file
    with open(LOGPATH + 'assemble.log', 'w'):
        pass

    # run the program
    """ with IB().connect(HOST, PORT, CID) as ib:
        df = ib.run(get_snp(ib)) \
            if MARKET.upper() == 'SNP' \
            else ib.run(get_nse(ib))

    df.to_pickle(FSPATH + 'df_symlots.pkl') """
    with IB().connect(HOST, PORT, CID) as ib:
        df = ib.run(assemble(ib, MARKET, FSPATH))

    print(df)
    print(
        f'Took {time.strftime("%H:%M:%S", time.gmtime(time.time()-start))}')
