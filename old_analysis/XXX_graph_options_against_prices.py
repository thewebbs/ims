#------------------------------------------------------------
# Filename  : graph_options_against_prices.py
# Project   : ava
#
# Descr     : This file contains code to graph options against prices for a given ticker
#
# Params    : database
#             inv_ticker
#             inv_exc_symbol
#             start_date
#             end_date
#             expiry_date
#             strike_price1
#             strike_price2
#             option_type
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-09-18   1 MW  Initial write
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_MARKER_KIND, VISUAL_MARKER_EDGE_COLOR, VISUAL_RED_LINE_COLOR, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db, sql_select_all
from datetime import datetime
from decimal import Decimal
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from utils.utils_dataframes import normalize_df
from sklearn.metrics.cluster.supervised import normalized_mutual_info_score


def add_to_df(in_df, displaytype, pri_date, pri_price):

    temp = pd.DataFrame({pri_date: (float(pri_price))},  
                             index=[displaytype] )
    
    in_df = pd.concat([in_df, temp])
    
    return in_df


def get_options_for_ticker_between_dates(database, inv_ticker, inv_exc_symbol, start_date, end_date, expiry_date, strike_price, option_type):

    options_df = pd.DataFrame() # for all records
    
    option_prices = get_option_prices_in_range_for_investment(database       = database,
                                                              inv_ticker     = inv_ticker, 
                                                              inv_exc_symbol = inv_exc_symbol, 
                                                              start_date     = start_date, 
                                                              end_date       = end_date, 
                                                              expiry_date    = expiry_date, 
                                                              strike_price   = strike_price, 
                                                              option_type    = option_type)
    if DEBUG:
        print(option_prices)
    
    option_name = 'Strike_' + str(strike_price) 
    for (option_date, midpoint_price) in option_prices:
        if midpoint_price == None:
            midpoint_price = Decimal(0)
        
        options_df = add_to_df(options_df, option_name, option_date, float(midpoint_price)) 
        
    options_df = options_df.groupby(options_df.index).sum()    
    options_df = options_df.transpose()
    
    if DEBUG:
        print(options_df)
        
    return options_df


def get_option_prices_in_range_for_investment(database, inv_ticker, inv_exc_symbol, start_date, end_date, expiry_date, strike_price, option_type):
  
    sql_statement =  "SELECT date_format(opp_datetime,'%Y-%m-%d %H:%i') as option_date, "
    sql_statement += " round(MAX(opp_bid_price + (opp_ask_price-opp_bid_price)/2),2) as midpoint_price "
    sql_statement += " from option_prices "
    sql_statement += " where opp_opc_inv_ticker ='%s' " % (inv_ticker)
    sql_statement += " and opp_opc_inv_exc_symbol = '%s' " % (inv_exc_symbol)
    sql_statement += " and DATE(opp_datetime) between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " and opp_opc_expiry_date = '%s' " % (expiry_date)
    sql_statement += " and opp_opc_option_type = '%s' " % (option_type)
    sql_statement += " and opp_strike_price = %s " % (strike_price)
    sql_statement += " group by date_format(opp_datetime,'%Y-%m-%d %H:%i') "
    sql_statement += " order by date_format(opp_datetime,'%Y-%m-%d %H:%i') "
    
    if DEBUG:
        print("get_option_prices_in_range_for_investment")

    option_prices = sql_select_all(database      = database,
                                   sql_statement = sql_statement)
    
    if DEBUG:
        print(option_prices)
        
    return option_prices


def get_prices_in_range_for_ticker_with_option_price(database, inv_ticker, inv_exc_symbol, start_date, end_date):
    
    sql_statement =  "SELECT date_format(can_start_datetime,'%Y-%m-%d %H:%i') as pri_date, "
    sql_statement += " round(can_last_bid_price+(can_last_ask_price-can_last_bid_price)/2,2) as midpoint_price "
    sql_statement += " from candlesticks "
    sql_statement += " where can_inv_ticker = '%s' " % (inv_ticker)
    sql_statement += " and can_inv_exc_symbol = '%s' " % (inv_exc_symbol)
    sql_statement += " and DATE(can_start_datetime) between '%s' and '%s' " % (start_date, end_date)
    sql_statement += " order by can_start_datetime"
    
    if DEBUG:
        print("get_prices_in_range_for_investment")

    prices = sql_select_all(database      = database,
                            sql_statement = sql_statement)
    
    if DEBUG:
        print(prices)
        
    return prices


def get_prices_for_ticker_between_dates(database, inv_ticker, inv_exc_symbol, start_date, end_date):

    prices_df = pd.DataFrame()     
    
    prices = get_prices_in_range_for_ticker_with_option_price(database       = database,
                                                              inv_ticker     = inv_ticker, 
                                                              inv_exc_symbol = inv_exc_symbol, 
                                                              start_date     = start_date, 
                                                              end_date       = end_date)
    if DEBUG:
        print(prices)

    for (pri_date, midpoint_price) in prices:
    
        if midpoint_price == None:
            midpoint_price = Decimal(0)
       
        prices_df = add_to_df(prices_df, 'Price', pri_date, float(midpoint_price)) 

    prices_df = prices_df.groupby(prices_df.index).sum()    
    prices_df = prices_df.transpose()
    
    if DEBUG:
        print(prices_df)
    
    return prices_df


def graph_options_against_prices(database, inv_ticker, inv_exc_symbol, start_date, end_date, expiry_date, strike_price1, strike_price2, option_type):
   
    prices_df = get_prices_for_ticker_between_dates(database       = database,
                                                    inv_ticker     = inv_ticker, 
                                                    inv_exc_symbol = inv_exc_symbol, 
                                                    start_date     = start_date, 
                                                    end_date       = end_date)
    
    options_df1 = get_options_for_ticker_between_dates(database       = database,
                                                       inv_ticker     = inv_ticker, 
                                                       inv_exc_symbol = inv_exc_symbol, 
                                                       start_date     = start_date, 
                                                       end_date       = end_date, 
                                                       expiry_date    = expiry_date, 
                                                       strike_price1  = strike_price1, 
                                                       option_type    = option_type)
    
    options_df2 = get_options_for_ticker_between_dates(database       = database,
                                                       inv_ticker     = inv_ticker, 
                                                       inv_exc_symbol = inv_exc_symbol, 
                                                       start_date     = start_date, 
                                                       end_date       = end_date, 
                                                       expiry_date    = expiry_date, 
                                                       strike_price1  = strike_price2, 
                                                       option_type    = option_type)
        
    if not(prices_df.empty) and not(options_df1.empty):
        
        # give the options_df the same indexes as the prices_df
        
        new_options_df    = pd.DataFrame(index=prices_df.index)   
        graph_options_df1 = pd.concat([new_options_df, options_df1], axis = 1)
        graph_options_df2 = pd.concat([new_options_df, options_df2], axis = 1)
        
        title1 = 'Graph showing option prices and underlying prices for ' + inv_ticker + '.' + inv_exc_symbol + ' between '  + start_date + ' and ' + end_date 
        title2 = " Strikes " + str(strike_price1) + " and " + str(strike_price2) + " Expiry " + expiry_date + " Type " + option_type 
        filename = "%sGRAPH_%s_%s_OPTIONS_VS_PRICES_FROM_%s_TO_%s_Strike_%s_%s_Expiry_%s_Type_%s.png" % (REPORT_FOLDER_OSX, inv_ticker, inv_exc_symbol, start_date[0:10], end_date[0:10], str(strike_price1), str(strike_price2), expiry_date, option_type)
        graph_dfs(prices_df, graph_options_df1, graph_options_df2, title1, title2, filename)
        plt.show()
    
    else:
        print('NO DATA FOUND')
    
    return
    

def graph_dfs(prices_df, graph_options_df1, graph_options_df2, title1, title2, filename):
    
    plt.style.use(VISUAL_STYLE_TO_USE) 
      
    fig,(ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True, figsize=(20,10))
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('Price', color=VISUAL_XAXIS_COLOR)       # this sets the axes title colour only
   
    plt.suptitle(title1, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    ax1.set_title('Underlying Prices', fontsize=VISUAL_2ND_TITLE_FONT_SIZE)
    
    if DEBUG:
        print('#######################')
        print('shape graph_options_df',graph_options_df1.shape)
        print('graph_options_df', graph_options_df1)
        print('shape graph_options_df',graph_options_df2.shape)
        print('graph_options_df', graph_options_df2)
        print('#######################')
    
    xticks=prices_df.index.values
    xticks_to_use=prices_df.index.values[::5]
    ax1.grid(linestyle="dotted", color='grey', linewidth=0.5)
    
    ax2.set_title(title2, fontsize=VISUAL_2ND_TITLE_FONT_SIZE)
    plt.rcParams['axes.grid'] = True
    prices_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    ax1.set_xticklabels(xticks_to_use, fontdict=None, minor=False)
    fig.autofmt_xdate()
      
    # normalized versions go on ax2 together
    
    if not(graph_options_df1.empty):
        normalized_graph_options_df1 = normalize_df(graph_options_df1)
        normalized_graph_options_df1.plot(ax=ax2, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = 'b')
            
    if not(graph_options_df2.empty):
        normalized_graph_options_df2 = normalize_df(graph_options_df2)
        normalized_graph_options_df2.plot(ax=ax2, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = 'g')
    
    ax2.grid(linestyle="dotted", color='grey', linewidth=0.5)
    ax2.set_xticklabels(xticks_to_use, fontdict=None, minor=False)
    fig.autofmt_xdate()
    
    # non normalized versions go on their own graph
    
    if not(graph_options_df1.empty):
        graph_options_df1.plot(ax=ax3, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = 'b')
    
    ax3.grid(linestyle="dotted", color='grey', linewidth=0.5)
    ax3.set_xticklabels(xticks_to_use, fontdict=None, minor=False)
    fig.autofmt_xdate()
    
    if not(graph_options_df2.empty):
        graph_options_df2.plot(ax=ax4, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = 'g')
    
    ax4.grid(linestyle="dotted", color='grey', linewidth=0.5)
    ax4.set_xticklabels(xticks_to_use, fontdict=None, minor=False)
    fig.autofmt_xdate()
    
    # now save the graphs to the specified pathname
    
    plt.savefig(filename)
 
    return
    
   
if __name__ == "__main__":
    
    print("Started graph_options_against_prices")
    print(" ")
    print("Open Database")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)   
    
    graph_options_against_prices(database       = database,
                                 inv_ticker     = 'CM', 
                                 inv_exc_symbol = 'TSE', 
                                 start_date     = '2018-08-25 06:30:00', 
                                 end_date       = '2018-08-28 12:59:59', 
                                 expiry_date    = '2018-09-21', 
                                 strike_price1  = 100, 
                                 strike_price2  = 110, 
                                 option_type    = 'C')  
        
    print("Close database")
    close_db(database = database)   
    
    print("Finished graph_options_against_prices") 
