#------------------------------------------------------------
# Filename  : graph_fill_between.py
# Project   : ava
#
# Descr     : This file contains code to plot the VIX
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-05-23   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.transforms as mtransforms

      
if __name__ == "__main__":
    
    url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS'
    vix = pd.read_csv(url, index_col=0, parse_dates=True, na_values='.', infer_datetime_format=True, squeeze=True).dropna()
    ma = vix.rolling('90d').mean()
    state = pd.cut(ma, bins=[-np.inf, 14, 18, 24, np.inf], labels=range(4))

    cmap = plt.get_cmap('RdYlGn_r')
    ma.plot(color='black', linewidth=1.5, marker='', figsize=(8, 4), label='VIX 90d MA')
    ax = plt.gca()  # get the current Axes that ma.plot() references
    ax.set_xlabel('')
    ax.set_ylabel('90d moving average: CBOE VIX')
    ax.set_title('Volatility Regime State')
    ax.grid(False)
    ax.legend(loc='upper center')
    ax.set_xlim(xmin=ma.index[0], xmax=ma.index[-1])

    trans = mtransforms.blended_transform_factory(ax.transData, ax.transAxes)
    for i, color in enumerate(cmap([0.2, 0.4, 0.6, 0.8])):
        ax.fill_between(ma.index, 0, 1, where=state==i, facecolor=color, transform=trans)
        ax.axhline(vix.mean(), linestyle='dashed', color='xkcd:dark grey', alpha=0.6, label='Full-period mean', marker='')
    plt.show()
