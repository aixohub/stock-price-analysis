import numpy as np
import pandas as pd
import cufflinks as cf
import plotly.offline as plyo

raw = pd.read_csv('./stock-NVDA.csv',
                  index_col=0, parse_dates=True)
print(raw.info())

quotes = raw[['open', 'high', 'low', 'close', 'volume']]
quotes = quotes.iloc[-60:]
qf = cf.QuantFig(
    quotes,
    title='EUR/USD Exchange Rate',
    legend='top',
    name='EUR/USD'
)

# qf.add_bollinger_bands(periods=15,  boll_std=2)
qf.add_rsi(periods=14, showbands=False)
qf.add_macd()
qf.add_volume()
plyo.iplot(
    qf.iplot(asFigure=True),
    image='png',
    filename='qf_01'
)
