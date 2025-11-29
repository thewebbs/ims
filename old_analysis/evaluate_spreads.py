#------------------------------------------------------------
# Filename  : evaluate_spreads.py
# Project   : ava
#
# Descr     : This file contains code to evaluate the spreads in ticker prices
#
# Params    : inv_ticker
#             inv_exc_symbol
#             start_date
#             end_date
#             produce_graph
#             display_to_screen
#             output_path_name
#
# History   :
#
# Date       Ver Who Change
# ---------- --- --- ------
# 2018-09-20   1 MW  Initial write 
# ...
# 2021-08-31 100 DW  Added version and moved to ILS-ava 
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from database.db_objects.CandlestickDB import get_hourly_bid_offer_spread

from utils.config import DEBUG, STREAK_LOOKBACK_PERIOD, STREAK_LOOKBACK_THRESHOLD, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_MARKER_KIND, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
from utils.config import STREAK_LOOKBACK_PERIOD, STREAK_LOOKBACK_THRESHOLD, VOLATILITY_MIN_PRICE, VOLATILITY_MAX_PRICE

from utils.utils_database import close_db, open_db
from utils.config import DEBUG, REPORT_FOLDER_OSX, DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD

from decimal import Decimal
from analysis.DetectStreaks import detectstreaks
from apis.dataload.load_drive_table import load_drive_table
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from utils.utils_dataframes import fixit_df
from database.db_objects import VolatilityDetailDB.VolatilityDetailDB
from database.db_objects  import VolatilityHeaderDB

number_ups   = 0
number_downs = 0
number_flats = 0
profit       = 0


# Get methods

def add_streak_info_to_df(in_df, inv_ticker, number_ups, number_downs, number_flats, this_profit):

    in_df.at[inv_ticker,'NUMBER_UPS']   = number_ups
    in_df.at[inv_ticker,'NUMBER_DOWNS'] = number_downs
    in_df.at[inv_ticker,'NUMBER_FLATS'] = number_flats
    in_df.at[inv_ticker,'PROFIT']       = this_profit
    
    if DEBUG:
        print('in add_streak_info_to_df')
        print(in_df)
    
    return in_df


def add_to_df(in_df, displaytype, pri_date, pri_price):

    temp = pd.DataFrame({pri_date: (float(pri_price))},  
                             index=[displaytype] )
    
    in_df = pd.concat([in_df, temp])
    
    return in_df


def add_to_volatility_details_table(start_date, end_date, inv_ticker, inv_exc_symbol, mean_spread, percent_spread, 
                                    mean_price, num_ups, num_downs, num_flats, profit):
    
    print('adding to add_to_volatility_details_table table')
    
    # loop through the investments in the data frame, inserting each one into the add_to_volatility_headers_table table
    
    newVolatilityDetail = VolatilityDetailDB(vdl_vhd_start_date                = start_date, 
                                             vdl_vhd_end_date                  = end_date, 
                                             vdl_vhd_min_price                 = VOLATILITY_MIN_PRICE, 
                                             vdl_vhd_max_price                 = VOLATILITY_MAX_PRICE, 
                                             vdl_vhd_streak_lookback_period    = STREAK_LOOKBACK_PERIOD, 
                                             vdl_vhd_streak_lookback_threshold = STREAK_LOOKBACK_THRESHOLD, 
                                             vdl_inv_ticker                    = inv_ticker, 
                                             vdl_inv_exc_symbol                = inv_exc_symbol,
                                             vdl_mean_spread                   = mean_spread, 
                                             vdl_percent_spread                = percent_spread, 
                                             vdl_mean_price                    = mean_price, 
                                             vdl_num_ups                       = num_ups, 
                                             vdl_num_downs                     = num_downs, 
                                             vdl_num_flats                     = num_flats, 
                                             vdl_profit                        = profit)
    newVolatilityDetail.write_DB()
            
    return


def add_to_volatility_headers_table(start_date, end_date):
    
    print('adding to add_to_volatility_headers_table table')
    
    # loop through the investments in the data frame, inserting each one into the add_to_volatility_headers_table table
    
    vhd_min_price = Decimal(VOLATILITY_MIN_PRICE)
    vhd_max_price = Decimal(VOLATILITY_MAX_PRICE)
    print(vhd_min_price, vhd_max_price)
    
    newVolatilityHeader = VolatilityHeadersDB(vhd_start_date                = start_date, 
                                              vhd_end_date                  = end_date, 
                                              vhd_min_price                 = vhd_min_price, 
                                              vhd_max_price                 = vhd_max_price, 
                                              vhd_streak_lookback_period    = STREAK_LOOKBACK_PERIOD, 
                                              vhd_streak_lookback_threshold = STREAK_LOOKBACK_THRESHOLD)
    
    newVolatilityHeader.write_DB()
            
    return


def build_dfs(inv_ticker, inv_exc_symbol, start_date, end_date):
   
    # get midpoint price data and build into price and date lists for processing
    
    prices     = []
    freq_type  = '1 min'
    prices_df  = pd.DataFrame()
    spreads_df = pd.DataFrame()
    
    prices[:] = get_hourly_bid_offer_spread(inv_ticker, inv_exc_symbol, start_date, end_date, freq_type, VOLATILITY_MIN_PRICE, VOLATILITY_MAX_PRICE)
   
    for (can_inv_ticker, can_inv_exc_symbol, can_end_datetime, bid_offer_spread, bid_price) in prices:
    
        if bid_offer_spread == None:
            bid_offer_spread = Decimal(0)
        if bid_price == None:
            bid_price = Decimal(0)
            
        this_investment = can_inv_ticker 
        prices_df       = add_to_df(prices_df, this_investment, can_end_datetime, float(bid_price)) 
        spreads_df      = add_to_df(spreads_df, this_investment, can_end_datetime, float(bid_offer_spread)) 
        
    prices_df  = prices_df.groupby(prices_df.index).sum()
    prices_df  = prices_df.transpose()
    spreads_df = spreads_df.groupby(spreads_df.index).sum()
    spreads_df = spreads_df.transpose()
         
    return prices_df, spreads_df


def calc_percentage_bid_offer_spread(row):
    
    # used in df.apply to calculate the percentage across the dataframe
    
    percentage_bid_offer_spread = 100 * (row['SPREAD'] / row ['MEAN'])
    
    return percentage_bid_offer_spread


def evaluate_spreads(inv_ticker, inv_exc_symbol, start_date, end_date, produce_graph, display_to_screen, output_path_name):
    
    print('Processing for ', inv_ticker, 'between ', start_date, 'and', end_date)
    
    # build the lists of the prices and dates and also a dataframe ready for graphing
    
    prices_df, spreads_df = build_dfs(inv_ticker, inv_exc_symbol, start_date, end_date)
    
    mean_price_s         = prices_df.mean(axis=0)
    mean_spread_s        = spreads_df.mean(axis=0)
    sorted_price_mean_s  = mean_price_s.sort_values(axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last')
    sorted_spread_mean_s = mean_spread_s.sort_values(axis=0, ascending=False, inplace=False, kind='quicksort', na_position='last')
    lowest100spreads     = sorted_spread_mean_s[:100]
    
    combined_df         = pd.concat([lowest100spreads, sorted_price_mean_s], axis=1)
    combined_df.columns = ['SPREAD', 'MEAN']
    
    # remove those not in the top 100
    
    output_df = combined_df[pd.notnull(combined_df['SPREAD'])]
    
    # add these to the volatility_headers table
    
    add_to_volatility_headers_table(start_date = start_date, 
                                    end_date   = end_date)
    
    # add extra columns to hold the streak information
 
    new_cols=['PERCENT_SPREAD', 'NUMBER_UPS','NUMBER_DOWNS','NUMBER_FLATS','PROFIT']
    
    for i in range(len(new_cols)):
        output_df[new_cols[i]]=np.nan
    
    output_df['PERCENT_SPREAD'] = output_df.apply(lambda row: calc_percentage_bid_offer_spread(row), axis=1)
    
    # work out the streak information
    
    for this_inv_ticker in output_df.index.values:
        
        this_inv_exc_symbol = 'TSE'
        display_to_screen   = True
        number_ups, number_downs, number_flats, this_profit = detectstreaks(this_inv_ticker, this_inv_exc_symbol, start_date, end_date, produce_graph, display_to_screen, output_path_name)
        
        output_df = add_streak_info_to_df(output_df, this_inv_ticker, number_ups, number_downs, number_flats, this_profit)
        
        # add these to the volatility_details table
        
        mean_spread    = output_df.get_value(this_inv_ticker,'SPREAD')
        percent_spread = output_df.get_value(this_inv_ticker,'PERCENT_SPREAD')
        mean_price     = output_df.get_value(this_inv_ticker,'MEAN')
        
        add_to_volatility_details_table(start_date     = start_date, 
                                        end_date       = end_date,
                                        inv_ticker     = this_inv_ticker, 
                                        inv_exc_symbol = this_inv_exc_symbol, 
                                        mean_spread    = mean_spread, 
                                        percent_spread = percent_spread, 
                                        mean_price     = mean_price, 
                                        num_ups        = number_ups, 
                                        num_downs      = number_downs, 
                                        num_flats      = number_flats, 
                                        profit         = this_profit)
    
    # Sort by descending profit
    
    sorted_output_df = output_df.sort_values(by='PROFIT', ascending=False)
    
    if produce_graph:    
        results = graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, prices_df, output_path_name)
    
    return sorted_output_df


def graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, prices_df, output_path_name):

    # graph the prices dataframe then save the figure

    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
        
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR)      # this sets the axes title colour only
    plt.ylabel('Bid Offer Spread', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
   
    # change title to use dollar threshold rather than streak percent
    
    maintitle = "%s.%s End of Day Spread between %s and %s " % (inv_ticker, inv_exc_symbol, start_date, end_date)
    plt.suptitle(maintitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    
    prices_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT)
    
    # way do plotting depends on how many plots handling at once - one or many
    
    box = ax1.get_position()
    ax1.set_position([box.x0, box.y0, box.width * VISUAL_LEGEND_BBOX_WIDTH, box.height])
    ax1.legend(loc='center left', bbox_to_anchor=VISUAL_LEGEND_BBOX_TO_ANCHOR, ncol=VISUAL_LEGEND_COLS, fancybox=VISUAL_LEGEND_FANCYBOX, fontsize=VISUAL_LEGEND_FONT_SIZE)
    
    # use every 20th records for the xticks
    
    ticks_to_use = prices_df.index[::5]           
    
    ax1.set_xticklabels(ticks_to_use, rotation=VISUAL_XAXIS_ROTATION)
    plt.grid(linestyle="dotted", color='grey', linewidth=0.5)
    
    # now save the graphs to the specified pathname
    
    disp_start_date = start_date[0:10]
    disp_end_date   = end_date[0:10]
    
    # change filename to include dollar threshold not percentage
 
    output_file_name = "%sEOD_bid_offer_spread_%s_%s_%s_%s.png" % \
        ( output_path_name, inv_ticker, inv_exc_symbol, disp_start_date, disp_end_date )

    plt.savefig(output_file_name)

    # Only actually display if boolean says so
    
    if display_to_screen:
        plt.show()
    
    return

      
if __name__ == "__main__":
    
    print("Started evaluate_streaks")
    print(" ")
    print("Open Database")
    print("-------------------------------")
     
    database = open_db(host        = DB_HOST, 
                       port        = DB_PORT, 
                       tns_service = DB_TNS_SERVICE, 
                       user_name   = DB_USER_NAME, 
                       password    = DB_PASSWORD)
        
    #
    # set up parameters
    #
    
    start_date        = '2018-09-10 00:00:00'
    end_date          = '2018-09-15 23:59:59'
    output_path_name  = REPORT_FOLDER_OSX
    display_to_screen = True
    produce_graph     = False
    inv_ticker        = '%'
    inv_exc_symbol    = '%'
    
    sorted_output_df  = evaluate_spreads(database          = database,
                                         inv_ticker        = inv_ticker,
                                         inv_exc_symbol    = inv_exc_symbol, 
                                         start_date        = start_date, 
                                         end_date          = end_date, 
                                         produce_graph     = produce_graph, 
                                         display_to_screen = display_to_screen, 
                                         output_path_name  = output_path_name)
    
    print('at very end lowest 100 spreads ')
    print(sorted_output_df)
        
    print("Close database")
    close_db(database = database)   
    
    print("Finished evaluate_streaks") 
