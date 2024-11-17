import numpy as np
import pandas as pd
import cufflinks as cf
import plotly.offline as plyo



a = np.random.standard_normal((250, 5)).cumsum(axis=0)
index = pd.date_range('2023-1-1',  freq='B', periods=len(a))
df = pd.DataFrame(100 + 5 * a, columns=list('abcde'), index=index)
df.head()

plyo.iplot(df[['a', 'b']].iplot(asFigure=True,
                          theme='polar',
                          title='A Time Series Plot',
                          xTitle='date',
                          yTitle='value',
                          mode={'a': 'markers', 'b': 'lines+markers'},
                          symbol={'a': 'circle', 'b': 'diamond'},
                          size=3.5, colors={'a': 'blue', 'b': 'magenta'}, filename='ply_01'))