#------------------------------------------------------------
# Filename  : graph_divs_against_prices.py
# Project   : ava
#
# Descr     : This file contains code to graph dividends against prices for a given ticker
#
# Params    : database
#             inv_ticker
#             inv_exc_symbol
#             start_date
#             end_date
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-09-14   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_MARKER_KIND, VISUAL_MARKER_EDGE_COLOR, VISUAL_RED_LINE_COLOR, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
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


def get_dividends_for_ticker_between_dates(database, inv_ticker, inv_exc_symbol, start_date, end_date):

    start_dividends_df = pd.DataFrame()      # for all records
    end_dividends_df   = pd.DataFrame()      # for all records
    
    dividends = get_divs_prices_in_range_for_investment(database       = database,
                                                        inv_ticker     = inv_ticker, 
                                                        inv_exc_symbol = inv_exc_symbol, 
                                                        start_date     = start_date, 
                                                        end_date       = end_date)
    
    if DEBUG:
        print(dividends)

    for (pri_date, pri_adj_close, div_per_share) in dividends:
        if pri_adj_close == None:
            pri_adj_close = Decimal(0)
       
        start_dividends_df = add_to_df(start_dividends_df, 'PRICE_AT_EXDIV', pri_date, pri_adj_close) 
        end_dividends_df   = add_to_df(end_dividends_df, 'PRICE_AFTER_DPS', pri_date, pri_adj_close-div_per_share) 
    
    start_dividends_df = start_dividends_df.groupby(start_dividends_df.index).sum()    
    start_dividends_df = start_dividends_df.transpose()
    end_dividends_df   = end_dividends_df.groupby(end_dividends_df.index).sum()    
    end_dividends_df   = end_dividends_df.transpose()
        
    if DEBUG:
        print(start_dividends_df)
        print(end_dividends_df)
        
    return start_dividends_df, end_dividends_df


def get_divs_prices_in_range_for_investment(database, inv_ticker, inv_exc_symbol, start_date, end_date):

    sql_statement =  "SELECT distinct div_exdiv_date, pri_adj_close, div_per_share "
    sql_statement += " FROM dividends, prices "
    sql_statement += " WHERE pri_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " AND pri_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += " AND pri_date between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " AND pri_inv_ticker = div_inv_ticker "
    sql_statement += " AND pri_inv_exc_symbol = div_inv_exc_symbol "
    sql_statement += " AND pri_date = div_exdiv_date "
    sql_statement += " AND div_declaration_date is not null "
    sql_statement += " ORDER BY div_exdiv_date" 
    
    if DEBUG:
        print("get_divs_prices_in_range_for_investment")

    div_prices = sql_select_all(database      = database,
                                sql_statement = sql_statement)
    
    if DEBUG:
        print('ticker ',inv_ticker,' div_prices', div_prices)
        print(div_prices)
    
    return div_prices


def get_prices_in_range_for_investment(database, inv_ticker, inv_exc_symbol, start_date, end_date):

    sql_statement =  "SELECT pri_inv_ticker, pri_inv_exc_symbol, pri_date, pri_adj_close "
    sql_statement += " FROM prices "
    sql_statement += " WHERE pri_inv_ticker like '%s' " % (inv_ticker)
    sql_statement += " AND pri_inv_exc_symbol like '%s' " % (inv_exc_symbol)
    sql_statement += " AND pri_date between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " ORDER BY pri_date" 
    
    if DEBUG:
        print("get_prices_in_range_for_investment")

    prices = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(prices)
        
    return prices


def get_prices_for_ticker_between_dates(database, inv_ticker, inv_exc_symbol, start_date, end_date):

    prices_df = pd.DataFrame() # for all records
    
    prices = get_prices_in_range_for_investment(database       = database,
                                                inv_ticker     = inv_ticker, 
                                                inv_exc_symbol = inv_exc_symbol, 
                                                start_date     = start_date, 
                                                end_date       = end_date)
    
    if DEBUG:
        print(prices)

    for (pri_inv_ticker, pri_inv_exc_symbol, pri_date, pri_adj_close) in prices:
    
        if pri_adj_close == None:
            pri_adj_close = Decimal(0)
       
        prices_df = add_to_df(prices_df, 'PRICES', pri_date, pri_adj_close) 

    prices_df = prices_df.groupby(prices_df.index).sum()    
    
    if DEBUG:
        print('ticker ',inv_ticker, 'prices_df ',prices_df)
        print(prices_df)
    
    return prices_df


def graph_divs_against_prices(database, inv_ticker, inv_exc_symbol, start_date, end_date):
    
 
    prices_df = get_prices_for_ticker_between_dates(database       = database,
                                                    inv_ticker     = inv_ticker, 
                                                    inv_exc_symbol = inv_exc_symbol, 
                                                    start_date     = start_date, 
                                                    end_date       = end_date)
    
    start_dividends_df, end_dividends_df = get_dividends_for_ticker_between_dates(database       = database,
                                                                                  inv_ticker     = inv_ticker, 
                                                                                  inv_exc_symbol = inv_exc_symbol, 
                                                                                  start_date     = start_date, 
                                                                                  end_date       = end_date)
    
    if not(start_dividends_df.empty) and not(end_dividends_df.empty):
        
        # join the dataframes together
    
        frames      = [prices_df, start_dividends_df]
        combined_df = pd.concat(frames, sort=False)
        combined_df = combined_df.transpose()
    
        if DEBUG:
            print('###############')
            print('combined_df', combined_df)
            print('start_dividends_df', start_dividends_df)
            print('end_dividends_df', end_dividends_df)
            print('###############')
        
        ttitle = 'Graph showing prices and ex div dates for ' + inv_ticker + '.' + inv_exc_symbol + ' between '  + start_date + ' and ' + end_date
        filename = "%sGRAPH_PRICE_EXDIV_%s_%s_FROM_%s_TO_%s.png" % (REPORT_FOLDER_OSX, inv_ticker, inv_exc_symbol, start_date[0:10], end_date[0:10])
        if not(combined_df.empty):
            graph_dfs(combined_df, start_dividends_df, end_dividends_df, ttitle, filename)
            plt.show()
        else:
            print('no data found for ticker', inv_ticker)
    else:
        print('no data found 2 for ticker ', inv_ticker) 
            
    return
    

def graph_dfs(combined_df, start_dividends_df, end_dividends_df, ttitle, filename):
    
    #figure_number = 1
    #fig, (ax1) = plt.subplots(figure_number, sharex=True, sharey=True)
    
    #styles = ['b-', 'r-', 'y-']
    
    #ax1.set_title(ttitle)
    
    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
        
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('Price', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
   
    plt.suptitle(ttitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    
    combined_df['PRICES'].plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    
    new_start_divs_df   = pd.DataFrame(index=combined_df.index)   
    graph_start_divs_df = pd.concat([new_start_divs_df, start_dividends_df], axis = 1, sort=False)
    
    new_end_divs_df     = pd.DataFrame(index=combined_df.index)   
    graph_end_divs_df   = pd.concat([new_end_divs_df, end_dividends_df], axis = 1, sort=False)
    
    graph_start_divs_df.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, marker=VISUAL_MARKER_KIND, markeredgecolor=VISUAL_COLOR_LIST[1], color=VISUAL_COLOR_LIST[1])
    
    graph_end_divs_df.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, marker=VISUAL_MARKER_KIND, markeredgecolor=VISUAL_RED_LINE_COLOR, color=VISUAL_RED_LINE_COLOR)
    
    # legend to right hand side outside of box
  
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
    ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)
    
    # use every 20th records for the xticks
    ticks_to_use = combined_df.index[::5]           
    
    ax1.set_xticklabels(ticks_to_use, rotation=VISUAL_XAXIS_ROTATION)
    plt.grid(linestyle="dotted", color='grey', linewidth=0.5)
    
    # now save the graphs to the specified pathname
    
    plt.savefig(filename)
 
    return
    
   
if __name__ == "__main__":
    
    print("Started graph_divs_against_prices")
    print(" ")
    print("Open Database")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)  
    
    graph_divs_against_prices(database = database, inv_ticker='ACO.X', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='FN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='VET', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='X', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='BEI.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='BIP.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='FNV', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='SU', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='TRP', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='CAR.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='PBH', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='MX', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='AEM', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='BEP.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='CM', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='BMO', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='BNS', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    graph_divs_against_prices(database = database, inv_ticker='RY', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    
    
    #graph_divs_against_prices(database = database, inv_ticker='CTC.A', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='CCA', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='FFH', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='CP', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='MKP', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='RNW', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='MFR.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='SJR.B', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='TRI', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='EMA', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='BDT', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='BEIN.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='INO.UN', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    #graph_divs_against_prices(database = database, inv_ticker='MRU', inv_exc_symbol= 'TSE', start_date='2017-01-01', end_date='2018-09-30')  
    
    print("Close database")
    close_db(database = database)   
    
    print("Finished graph_divs_against_prices") 