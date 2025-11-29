#------------------------------------------------------------
# Filename  : detect_streaks.py
# Project   : ava
#
# Descr     : This file contains code to detect streaks in ticker prices
#
# Params    : database  
#             inv_ticker
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
# 2018-05-23   1 MW  Initial write from Dave's pseudo code
# ...
# 2021-09-05 100 DW  Added version and moved to ILS-ava
# 2022-11-05 200 DW  Reorg
#------------------------------------------------------------

from utils.config import DEBUG, STREAK_LOOKBACK_PERIOD, STREAK_LOOKBACK_THRESHOLD, VISUAL_COLOR_LIST, VISUAL_GRAPH_KIND, VISUAL_LEGEND_BBOX_TO_ANCHOR
from utils.config import VISUAL_LEGEND_BBOX_WIDTH, VISUAL_LEGEND_COLS, VISUAL_LEGEND_FANCYBOX, VISUAL_LEGEND_FONT_SIZE, VISUAL_GREY_LINE_COLOR
from utils.config import VISUAL_LINE_STYLE, VISUAL_LINE_WEIGHT, VISUAL_STYLE_TO_USE, VISUAL_TOP_TITLE_FONT_SIZE
from utils.config import VISUAL_2ND_TITLE_FONT_SIZE, VISUAL_XAXIS_COLOR, VISUAL_XAXIS_ROTATION, REPORT_FOLDER_OSX
from utils.config import DB_HOST, DB_PORT, DB_TNS_SERVICE, DB_USER_NAME, DB_PASSWORD
from utils.utils_database import close_db, open_db
from decimal import Decimal
import matplotlib.pyplot as plt
import pandas as pd
from database.db_objects.ImsHistMktDataDB import get_hist_mkt_data_in_range
from database.db_objects.ImsStreakDetailDB import delete_these_streak_details, ImsStreakDetailDB
from database.db_objects.ImsStreakSummarieDB import delete_these_streak_summaries, get_streak_summary_id, ImsStreakSummarieDB
from utils.utils_dataframes import fixit_df

number_ups   = 0
number_downs = 0
number_flats = 0
profit       = 0

# Get methods

def buildlists(database, inv_ticker, inv_exc_symbol, start_date, end_date):
   
    price_list = []
    date_list  = []
    
    # get midpoint price data and build into price and date lists for processing
    
    hist_mkt_prices = []
    freq_type       = '1 min'
    prices_df       = pd.DataFrame()
    
    # note that in the database inv_ticker includes the exchange symbol
    
    hmd_inv_ticker     = inv_ticker + '.' + inv_exc_symbol
    hist_mkt_prices[:] = get_hist_mkt_data_in_range(database           = database,
                                                    hmd_inv_ticker     = hmd_inv_ticker, 
                                                    hmd_start_datetime = start_date, 
                                                    hmd_end_datetime   = end_date, 
                                                    hmd_freq_type      = freq_type)
    
    for (start_datetime, bid_price, ask_price) in hist_mkt_prices:
        
        if bid_price == None:
            bid_price = Decimal(0)
        
        if ask_price == None:
            ask_price = Decimal(0)
        
        midpoint_price = bid_price + ((ask_price - bid_price)/2)   
        
        price_list.append(midpoint_price)
        date_list.append(start_datetime)
    
        # record price record in the prices dataframe which contains all records
                        
        index_val = inv_ticker + "-" + inv_exc_symbol 
        
        format_date = start_datetime.strftime('%m/%d %H:%M')
        temp_df     = pd.DataFrame({format_date: (float(midpoint_price))}, index=[index_val] )
    
        # added sort=False for future behavior
        #prices_df = pd.concat([prices_df, temp_df]) 
        
        prices_df = pd.concat([prices_df, temp_df], sort=False) 
            
    prices_df = prices_df.groupby(prices_df.index).sum()
    
    if len(prices_df.index) > 0:  
        prices_df = fixit_df(prices_df)
         
    return price_list, date_list, prices_df


def delete_existing_streaks(database, sts_inv_ticker, sts_inv_exc_symbol, start_datetime, end_datetime, sts_threshold):
    
    # because the key sequence is auto increment we set it to null here
    
    these_sts_ids    = []
    these_sts_ids[:] = get_streak_summary_id(database           = database,
                                             sts_inv_ticker     = inv_ticker, 
                                             sts_inv_exc_symbol = inv_exc_symbol, 
                                             start_date         = start_datetime, 
                                             end_date           = end_datetime, 
                                             sts_threshold      = STREAK_LOOKBACK_THRESHOLD)
    
    # now loop through the these_sts_id as there will be many and delete them
    
    for this_sts_id in these_sts_ids:
        
        # first delete the details
        
        result = delete_these_streak_details(database        = database,
                                             std_sts_id      = this_sts_id, 
                                             start_datetime  = start_datetime,
                                             end_datetime    = end_datetime, 
                                             std_streak_type = '%')
        
        # then delete the summary itself
        
        result = delete_these_streak_summaries(database           = database,
                                               sts_inv_ticker     = sts_inv_ticker, 
                                               sts_inv_exc_symbol = sts_inv_exc_symbol, 
                                               start_date         = start_datetime, 
                                               end_date           = end_datetime, 
                                               sts_threshold      = STREAK_LOOKBACK_THRESHOLD)
        
    return 


def detect_streaks(database, inv_ticker, inv_exc_symbol, start_date, end_date, produce_graph, display_to_screen, output_path_name):
    
    global number_ups
    global number_downs
    global number_flats
    global profit
    
    number_ups   = 0
    number_downs = 0
    number_flats = 0
    profit       = 0
    
    print('=======================================================================')   
    print('Processing for ', inv_ticker, 'between ', start_date, 'and', end_date)
    
    # first delete any existing
    
    delete_existing_streaks(database           = database,
                            sts_inv_ticker     = inv_ticker, 
                            sts_inv_exc_symbol = inv_exc_symbol, 
                            start_datetime     = start_date, 
                            end_datetime       = end_date,
                            sts_threshold      = STREAK_LOOKBACK_THRESHOLD)
    
    # now start looking for streaks
 
    current_index      = 1
    streak_start_index = current_index
    current_direction  = 'flat'
    streaks_df         = pd.DataFrame()
    
    # build the lists of the prices and dates and also a dataframe ready for graphing
    
    price_list, date_list, prices_df = buildlists(database       = database,
                                                  inv_ticker     = inv_ticker,
                                                  inv_exc_symbol = inv_exc_symbol, 
                                                  start_date     = start_date, 
                                                  end_date       = end_date)
    number_recs = len(price_list)
    
    # only proceed if data was returned
    
    if number_recs == 0:

        if DEBUG:
            print('No records returned for this ticker, exc_symbol, date range')
        
        number_ups   = 0
        number_downs = 0 
        number_flats = 0
        this_profit  = 0
        
    else:    
        
        for current_index in range(number_recs):
            
            next_direction = get_direction(price_list        = price_list, 
                                           date_list         = date_list, 
                                           current_index     = current_index, 
                                           current_direction = current_direction)
            
            if next_direction != current_direction:
               
                if streak_has_ended(price_list         = price_list, 
                                    date_list          = date_list, 
                                    current_index      = current_index, 
                                    current_direction  = current_direction, 
                                    lookback_period    = STREAK_LOOKBACK_PERIOD, 
                                    lookback_threshold = STREAK_LOOKBACK_THRESHOLD):
                    
                    # Note that need to look at the record immediately before the Current_index as the end of the streak
                    # not the Current_index
                    
                    streak_end_index = current_index-1
                    
                    start_price    = price_list[streak_start_index]
                    end_price      = price_list[streak_end_index]
                    start_datetime = date_list[streak_start_index]
                    end_datetime   = date_list[streak_end_index]
                    
                    streaks_df     = record_streak(start_index       = streak_start_index, 
                                                   end_index         = streak_end_index, 
                                                   start_price       = start_price, 
                                                   end_price         = end_price, 
                                                   start_datetime    = start_datetime, 
                                                   end_datetime      = end_datetime, 
                                                   direction         = current_direction, 
                                                   streaks_df        = streaks_df, 
                                                   inv_ticker        = inv_ticker, 
                                                   inv_exc_symbol    = inv_exc_symbol)
   
                    current_direction  = next_direction
                    streak_start_index = current_index 
                    
        # Don't forget the last one
        
        start_price    = price_list[streak_start_index]
        end_price      = price_list[current_index]
        start_datetime = date_list[streak_start_index]
        end_datetime   = date_list[current_index]
        
        streaks_df     = record_streak(start_index    = streak_start_index, 
                                       end_index      = current_index, 
                                       start_price    = start_price, 
                                       end_price      = end_price, 
                                       start_datetime = start_datetime, 
                                       end_datetime   = end_datetime, 
                                       direction      = current_direction, 
                                       streaks_df     = streaks_df, 
                                       inv_ticker     = inv_ticker, 
                                       inv_exc_symbol = inv_exc_symbol)
    
        streaks_df = streaks_df.groupby(streaks_df.index).sum()
        
        if len(streaks_df.index) > 0:  
            streaks_df = fixit_df(streaks_df)
    
        results = "Number Ups: %2.0f  Number Downs: %2.0f  Number Flats: %2.0f  Profit: $%2.2f" % (number_ups, number_downs, number_flats, profit)
        print(results)
        
        if produce_graph:
            results = graph_results(inv_ticker        = inv_ticker, 
                                    inv_exc_symbol    = inv_exc_symbol, 
                                    start_date        = start_date, 
                                    end_date          = end_date, 
                                    prices_df         = prices_df, 
                                    streaks_df        = streaks_df, 
                                    output_path_name  = output_path_name, 
                                    profit            = profit, 
                                    display_to_screen = display_to_screen, 
                                    number_ups        = number_ups, 
                                    number_downs      = number_downs, 
                                    number_flats      = number_flats)
        
        this_profit = profit
    
    return number_ups, number_downs, number_flats, this_profit


def get_direction(price_list, date_list, current_index, current_direction):

    New_direction = current_direction
    if current_index > 1:
        
        # we will not go before start of datastructure
        
        previous_value = price_list[current_index-1]
        current_value  = price_list[current_index]

        if current_value > previous_value:
            new_direction = 'up'
        elif current_value < previous_value:
            new_direction = 'down'
        else:
            new_direction = 'flat'
    else:
        new_direction = 'flat'

    if DEBUG:
        if current_index > 1:
            print('In get_direction, current_direction = ', current_direction,' previous_value = ', previous_value,' Current_value = ', current_value, ' new_direction =', new_direction)
    
    return new_direction


def graph_results(inv_ticker, inv_exc_symbol, start_date, end_date, prices_df, streaks_df, output_path_name, profit, display_to_screen, number_ups, number_downs, number_flats):

    # graph the prices dataframe then save the figure

    plt.style.use(VISUAL_STYLE_TO_USE) 
    
    fig = plt.figure(figsize=(20,10))
    ax1 = fig.add_subplot(111)
        
    plt.xlabel('Date & Time', color=VISUAL_XAXIS_COLOR) # this sets the axes title colour only
    plt.ylabel('Price', color=VISUAL_XAXIS_COLOR)       # this sets the axes title colour only
   
    # change title to use dollar threshold rather than streak percent
    
    maintitle = "%s.%s Streaks between %s and %s using %3.2f dollar threshold looking back %1.0f records with profit $%5.2f" % \
        (inv_ticker, inv_exc_symbol, start_date, end_date, STREAK_LOOKBACK_THRESHOLD, STREAK_LOOKBACK_PERIOD, profit)
    plt.suptitle(maintitle, fontsize=VISUAL_TOP_TITLE_FONT_SIZE)
    lowertitle = "Number Ups: %2.0f  Number Downs: %2.0f  Number Flats: %2.0f" % (number_ups, number_downs, number_flats)
    plt.title(lowertitle, fontsize=VISUAL_2ND_TITLE_FONT_SIZE)
    
    prices_df.plot(ax=ax1, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT, color = VISUAL_GREY_LINE_COLOR)
    
    total_streaks = len(streaks_df.index)
    if total_streaks > 0:
        new_streaks_df = pd.DataFrame(index=prices_df.index)   
            
        # next line then puts the streak data frame this TransactionDB onto the same dataframe
        # added sort=False for future behavior
        #graph_streaks_df1 = pd.concat([new_streaks_df, streaks_df], axis = 1)
        
        graph_streaks_df1 = pd.concat([new_streaks_df, streaks_df], axis = 1, sort=False)
    
        # next line interpolate - forward which fills in the gap after the first record
        # pro-rating values after it til it reaches the end 
        # sadly it continues to use this value right until the end which the second interpolate statement fixs
        
        graph_streaks_df2 = graph_streaks_df1.interpolate(method = 'linear', limit_direction='forward')
        graph_streaks_df3 = graph_streaks_df2.interpolate(axis=0).where(graph_streaks_df1.bfill(axis=0).notnull())
        graph_streaks_df3.plot(ax=ax1, kind=VISUAL_GRAPH_KIND, linestyle=VISUAL_LINE_STYLE, lw=VISUAL_LINE_WEIGHT*2, color=VISUAL_COLOR_LIST[2])  # red
   
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
    
    output_file_name = "%sSTREAKS_%s_%s_%s_%s_%2.2f_%1.0f_$%5.2f.png" % \
        (output_path_name, inv_ticker, inv_exc_symbol, disp_start_date, disp_end_date, STREAK_LOOKBACK_THRESHOLD, STREAK_LOOKBACK_PERIOD, profit )

    plt.savefig(output_file_name)

    # Only actually display if boolean says so
    
    if display_to_screen:
        plt.show()

    
    return


def record_streak(start_index, end_index, start_price, end_price, start_datetime, end_datetime, direction, streaks_df, inv_ticker, inv_exc_symbol):

    global number_ups
    global number_downs
    global number_flats
    global profit

    # work out the date less the time for the summary record
    
    this_date = start_datetime.date()
    
    this_profit = 0
    
    if direction == 'up':
        number_ups += 1
        this_profit += (end_price - start_price) * 100
        
    if direction == 'down':
        number_downs += 1
        this_profit += (start_price - end_price) * 100
        
    if direction == 'flat':
        number_flats += 1
        this_profit += 0
    
    profit += this_profit
    
    # write a record to streak_summaries here 
    
    this_sts_id           = ''
    this_inv_ticker       = inv_ticker + '.' + inv_exc_symbol
    this_streak_summaries = ImsStreakSummarieDB(database           = database,
                                                sts_id             = this_sts_id, 
                                                sts_inv_ticker     = this_inv_ticker, 
                                                sts_inv_exc_symbol = inv_exc_symbol, 
                                                sts_date           = this_date, 
                                                sts_threshold      = STREAK_LOOKBACK_THRESHOLD)
    
    this_streak_summaries.insert_DB()
    
    if DEBUG:
        print('****** recording this is a streak *****')
        print('direction ', direction )
        print('start_index ', start_index, ' end_index ', end_index )
        print('start_price ', start_price, ' end_price ', end_price )
        print('start_datetime ', start_datetime, ' end_datetime ', end_datetime )
        print('profit = ', this_profit)
        print('***************************************')
  
    # write a record to streak_details here
   
    this_streak_detail = ImsStreakDetailDB(database                  = database,
                                           std_sts_id                = this_streak_summaries.sts_id, 
                                           std_streak_start_datetime = start_datetime, 
                                           std_streak_end_datetime   = end_datetime, 
                                           std_streak_type           = direction, 
                                           std_start_bid_price       = start_price, 
                                           std_start_ask_price       = start_price, 
                                           std_end_bid_price         = end_price, 
                                           std_end_ask_price         = end_price, 
                                           std_start_spread          = (end_price - start_price), 
                                           std_end_spread            = (end_price - start_price), 
                                           std_profit                = this_profit)
    
    this_streak_detail.insert_DB()
    
    #streak_index_val = inv_ticker + "-" + inv_exc_symbol + '-' + Direction
    
    index_val   = inv_ticker + "-" + inv_exc_symbol 
    format_date = start_datetime.strftime('%m/%d %H:%M')

    if len(streaks_df.index) == 0:   
        format_date = start_datetime.strftime('%m/%d %H:%M')
        temp_df     = pd.DataFrame({format_date: (float(start_price))}, index=[index_val] )
        streaks_df  = pd.concat([streaks_df, temp_df], sort=False)
 
    format_date = end_datetime.strftime('%m/%d %H:%M')
    temp_df     = pd.DataFrame({format_date: (float(end_price))}, index=[index_val] )
    streaks_df  = pd.concat([streaks_df, temp_df], sort=False)
     
    return streaks_df


def streak_has_ended(price_list, date_list, current_index, current_direction, lookback_period, lookback_threshold):

    ended = False

    if current_index >= lookback_period:

        # we will not go before start of datastructure
        
        lookback_value = price_list[current_index - lookback_period]
        current_value = price_list[current_index]

        if current_direction == 'up':
            if current_value < lookback_value:
                if (lookback_value - current_value) > lookback_threshold:
                    ended = True

        if current_direction == 'down':
            if current_value > lookback_value:
                if (current_value - lookback_value) > lookback_threshold:
                    ended = True

        if current_direction == 'flat':
            if abs(lookback_value - current_value) >= lookback_threshold:
                ended = True

    if DEBUG:
        
        if current_index >= lookback_period:
            if ended:
                print('In streak_has_ended, Lookback_value = ', lookback_value,' Current_value = ', current_value)
    
    return ended

      
if __name__ == "__main__":
    
    print("Started detect_streaks")
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
    
    inv_ticker        = 'CM'
    inv_exc_symbol    = 'TSE'
    start_date        = '2017-01-03 01:00:00'
    end_date          = '2017-01-03 23:23:59'
    output_path_name  = REPORT_FOLDER_OSX
    display_to_screen = True
    produce_graph     = True
    from_drive_table  = True
    
    number_ups, number_downs, number_flats, this_profit = detect_streaks(database          = database, 
                                                                         inv_ticker        = inv_ticker,
                                                                         inv_exc_symbol    = inv_exc_symbol, 
                                                                         start_date        = start_date, 
                                                                         end_date          = end_date, 
                                                                         produce_graph     = produce_graph, 
                                                                         display_to_screen = display_to_screen, 
                                                                         output_path_name  = output_path_name)
    
    print("Close database")
    close_db(database)   
    
    print("Finished DetectStreaks") 
