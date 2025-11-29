#------------------------------------------------------------
# Filename  : graph_end_of_day_prices_for_ticker_in_range.py
# Project   : ava
#
# Descr     : This file contains code to graph end of day bid prices for a specific ticker
#             in a specific date range
#
# Params    : None
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2020-04-16   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG
from utils.config import VISUAL_GREY_LINE_COLOR, VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_XAXIS_COLOR, VISUAL_YAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import open_db, close_db, sql_select_all
from decimal import Decimal
import matplotlib.pyplot as plt
import pandas as pd


def add_to_df(in_df, displaytype, pri_date, pri_price):

    format_date = pri_date.strftime('%y%m%d')
    temp = pd.DataFrame({format_date: (float(pri_price))},  
                             index=[displaytype] )
    
    in_df = pd.concat([in_df, temp], sort=False)
    
    return in_df


def get_eod_price_for_ticker_between_dates(database, inv_ticker, price_type, start_date, end_date):

    prices_df = pd.DataFrame()      # for all records
    
    prices = get_eod_price_for_ticker_in_range(database       = database,
                                               inv_ticker     = inv_ticker, 
                                               price_type     = price_type,
                                               start_date     = start_date, 
                                               end_date       = end_date)
    
    if DEBUG:
        print(prices)

    for (pri_date, pri_price) in prices:
        if pri_price == None:
            pri_price = Decimal(0)
       
        prices_df = add_to_df(prices_df, 'PRICES', pri_date, pri_price) 
            
    prices_df = prices_df.groupby(prices_df.index).sum()    
    prices_df = prices_df.transpose()
    
    if DEBUG:
        print(prices_df)
                
    return prices_df


def get_eod_price_for_ticker_in_range(database, inv_ticker, price_type, start_date, end_date):

    sql_statement =  "SELECT hmd_end_datetime,  "
    
    if price_type == 'BID':
        sql_statement += " hmd_last_bid_price "
    else:
        sql_statement += " hmd_last_ask_price "
        
    sql_statement += " FROM ims_hist_mkt_data "
    sql_statement += " WHERE hmd_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " AND hmd_end_datetime between to_date('%s 00:00:00','YYYY-MM-DD HH24:MI:SS') " % (start_date)
    sql_statement += "                          AND to_date('%s 12:59:59','YYYY-MM-DD HH24:MI:SS') " % (end_date)
    sql_statement += " AND to_char(hmd_end_datetime,'HH24:MI:SS') = '12:59:59' "
    sql_statement += " ORDER BY hmd_end_datetime" 
    
    if DEBUG:
        print("get_eod_price_for_ticker_in_range")

    prices = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print('ticker ',inv_ticker,' prices', prices)
        print(prices)
    
    return prices


def graph_end_of_day_prices_for_ticker_in_range(database, inv_ticker, price_type, start_date, end_date):
    
    
    prices_df = get_eod_price_for_ticker_between_dates(database       = database,
                                                    inv_ticker     = inv_ticker, 
                                                    price_type     = price_type, 
                                                    start_date     = start_date, 
                                                    end_date       = end_date)
    
    if not(prices_df.empty):
        
        ttitle = 'Graph showing End Of Day ' + price_type + ' prices for ' + inv_ticker + ' between '  + start_date + ' and ' + end_date
        filename = "%sGRAPH_EOD_%s_PRICE_%s_FROM_%s_TO_%s.png" % (REPORT_FOLDER_OSX, price_type, inv_ticker, start_date[0:10], end_date[0:10])
        print('filename',filename)
        graph_df(prices_df, ttitle, filename)
        plt.show()
        
    else:
        print('no data found for ticker ', inv_ticker) 
            
    return
    

def graph_df(prices_df, ttitle, filename):
    
    #figure_number = 1
    #fig, (ax1) = plt.subplots(figure_number, sharex=True, sharey=True)
    
    
    
    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
         
    plt.xlabel('Date', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('Price', color=VISUAL_YAXIS_COLOR) # this sets the axes title colour only
   
    plt.suptitle(ttitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    
    prices_df['PRICES'].plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    
    # use every 20th records for the xticks
    ticks_to_use = prices_df.index[::5]           
    
    ax1.set_xticklabels(ticks_to_use, rotation=VISUAL_XAXIS_ROTATION)
    plt.grid(linestyle="dotted", color='grey', linewidth=0.5)
    
    # now save the graphs to the specified pathname
    
    plt.savefig(filename)
 
    return
    
   
if __name__ == "__main__":
    
    print("Started graph_end_of_day_prices_for_ticker_in_range")
    print(" ")
    print("Open Database")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)  
    
    
    ticker_list = ['XIU.TO','CM.TO','BEP.UN.TO','FIE.TO','CHP.UN.TO','NVU.UN.TO','NWH.UN.TO', 'XRE.TO','RNW.TO','ENB.TO','HMMJ.TO','NWC.TO','SRJ.B.TO','AW.UN.TO','PZA.TO','INO.UN.TO','HR.UN.TO','AFN.TO','AD.TO']
    
    for this_ticker in ticker_list:
        graph_end_of_day_prices_for_ticker_in_range(database = database, inv_ticker=this_ticker, price_type = 'BID', start_date='2020-01-01', end_date='2020-04-16')  
        
    
    print("Close database")
    close_db(database = database)   
    
    print("Finished graph_end_of_day_prices_for_ticker_in_range") 