import numpy as np
import pandas as pd
import ydata.quality
import datetime
import numpy as np
import pandas as pd


def clean_data(df):
    """_summary_
    """
    #Drop NA values
    df['P'] = df['average_item_price_w_discount']
    df['Q'] = df['total_units_sold'] 

    df = df[df['P']>0]
    df = df[df['Q']>0]

    df['logP'] = np.log(df['average_item_price_w_discount'])
    df['logQ'] = np.log(df['total_units_sold'])

    df = df.dropna(subset=['Q','P'])

    ## Quey deal of the day query;
    dotd_history = psql.query("dotd_history",params=osr_params)
    if dotd_history.shape[0] == 0: pass
    else: pass

    dotd_history = dotd_history.set_index(['asin','date'], drop=False)
    df = df.set_index(['asin','date'], drop=False)
    df = df.drop(dotd_history.index, errors='ignore')
    return df