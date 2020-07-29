# Simulate a big qualifyAsync
import asyncio

import ib_insync as ib
import pandas as pd
import yaml

MARKET = 'snp'
root_path = "C:/Users/User/Documents/Business/Projects/asyncib/"
data_path = root_path+'data/'+MARKET.lower()+'/'

# get the connection IDs
with open(root_path+'var.yml') as f:
    data=yaml.safe_load(f)
    
HOST = data["COMMON"]["HOST"]
PORT = data[MARKET.upper()]["PORT"]
CID = data["COMMON"]["CID"]

# make the contract chains
df_chains = pd.read_pickle(data_path+'df_chains.pkl')

puts = [ib.Option(symbol=s, lastTradeDateOrContractMonth=e, strike=k, right='P', exchange=x) 
                for s, e, k, x in 
                zip(df_chains.symbol, df_chains.expiry, df_chains.strike, ['NSE' 
                    if MARKET.upper() == 'NSE' else 'SMART']*len(df_chains))]

calls = [ib.Option(symbol=s, lastTradeDateOrContractMonth=e, strike=k, right='C', exchange=x) 
                for s, e, k, x in 
                zip(df_chains.symbol, df_chains.expiry, df_chains.strike, ['NSE' 
                    if MARKET.upper() == 'NSE' else 'SMART']*len(df_chains))]

raw_cs = puts+calls

print(len(raw_cs))
