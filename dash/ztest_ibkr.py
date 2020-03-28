# test to work with some ibkr data

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objects as go

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# import ibkr data
MARKET = 'NSE'
SYMBOL = 'AMBUJACEM'

df_ohlcs = pd.read_pickle("C:/Users/User/Documents/ibkr/data/"+MARKET.lower()+"/ohlcs.pkl")
df = df_ohlcs[df_ohlcs.symbol==SYMBOL]

# prepare the data
data = go.Ohlc(
    x=df.date,
    open=df.open,
    high=df.high,
    low=df.low,
    close=df.close)

# prepare the figure
fig = go.Figure(data=data, with layouts
                layout_title_text=f"OHLC for {SYMBOL}",
                layout_title_xref='paper',
                layout_xaxis_title='Date',
                layout_yaxis_title='$'
)       


fig.show()


# app.layout = html.Div()
